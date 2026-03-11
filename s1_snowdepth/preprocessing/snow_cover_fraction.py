from pathlib import Path
import xarray as xr
import rioxarray

from s1_snowdepth.config import Config
from s1_snowdepth.download.ims import download_ims

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