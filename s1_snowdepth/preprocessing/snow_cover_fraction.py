from pathlib import Path
import xarray as xr
import rioxarray
from datetime import datetime, timedelta

from s1_snowdepth.config import Config
from s1_snowdepth.download.ims import download_ims
from s1_snowdepth.download.modis import download_modis

def load_ims(ims_path: Path, cfg: Config) -> xr.DataArray:
    """
    TODO: docs
    :param ims_path:
    :param cfg:
    :return:
    """
    ds = xr.open_dataset(ims_path)
    ims = (ds["IMS_Surface_Values"] == 4).astype(float)

    # Drop time dimension
    if "time" in ims.dims:
        ims = ims.squeeze("time")

    # Project to polar stereographic CRS
    ims_crs = "+proj=stere +lat_0=90 +lat_ts=60 +lon_0=-80 +k=1 +x_0=0 +y_0=0 +a=6378137 +b=6356257 +units=m"
    ims.rio.write_crs(ims_crs, inplace=True)

    # Clip to MODIS domain
    ims = ims.rio.clip_box(
        minx=cfg.gee_search_bbox[0],
        miny=cfg.gee_search_bbox[1],
        maxx=cfg.gee_search_bbox[2],
        maxy=cfg.gee_search_bbox[3],
        crs="EPSG:4326" # Reproject bounding box
    )

    return ims


def load_modis(modis_path: Path, cfg: Config) -> xr.DataArray:
    """
    Load MODIS MOD10A1 fractional snow cover GeoTIFF as xr.DataArray.

    :param modis_path: Path to modis_scf_YYYYMMDD.tif
    :return: xr.DataArray of MODIS data
    """
    if not modis_path.exists():
        date = modis_path.stem.split("_")[-1]
        print(f"No MODIS file found for {date}, downloading...")
        download_modis(date, modis_path.parent, cfg)

    return rioxarray.open_rasterio(modis_path).squeeze()


def gap_fill_modis(date: str, modis_dir: Path, cfg: Config, lookback_days: int = 5) -> xr.DataArray:
    """
    Gap-fill MODIS cloud pixels using inverse-distance weighted average of previous 5 days.
    Weighted with [5,4,3,2,1] -- the most recent day is weighted highest.
    For more details see Dunmire et al. 2024.

    :param date: Date to calculate SCF for (YYYYMMDD format)
    :param modis_dir: Directory containing MODIS GeoTIFF data for all days
    :param cfg: Model configuration defined in config.py and set in .env
    :param lookback_days: Number of days to use for gap-filling

    :return: Gap-filled MOD10A1 fractional snow cover DataArray.
    """
    date_dt = datetime.strptime(date, "%Y%m%d")
    weights = list(range(lookback_days, 0, -1))

    weighted_sum = None
    weight_total = None

    for i, w in enumerate(weights):
        d = (date_dt - timedelta(days=i)).strftime("%Y%m%d")
        modis_path = modis_dir / f"modis_scf_{d}.tif"

        if not modis_path.exists():
            print(f"No MODIS file found for {d}, downloading...")
            download_modis(d, modis_dir, cfg)

        modis = load_modis(modis_path, cfg)
        valid = modis.notnull()

        weighted_layer = modis.where(valid) * w
        weight_layer = xr.where(valid, w, 0)

        weighted_sum = (
            weighted_layer if weighted_sum is None
            else weighted_sum.fillna(0) + weighted_layer.fillna(0)
        )
        weight_total = (
            weight_layer if weight_total is None
            else weight_total + weight_layer
        )

    return weighted_sum / weight_total.where(weight_total > 0)


def compute_scf(date: str, ims_dir: Path, cfg: Config) -> xr.DataArray:
    """
    TODO: docs
    :param date:
    :param ims_dir:
    :param cfg:
    :return:
    """
    ims_path = download_ims(date, ims_dir, cfg)
    ims = load_ims(ims_path, cfg)

    # TODO: modis

    return ims