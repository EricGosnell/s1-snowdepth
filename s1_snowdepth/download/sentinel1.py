import asf_search as asf
from pathlib import Path
from datetime import datetime, timedelta
import zipfile
import re
import numpy as np
import xarray as xr
import rioxarray
from rasterio.enums import Resampling
from hyp3_sdk import HyP3, Batch

from s1_snowdepth.config import Config


def search_s1_scenes(start_date: datetime, end_date: datetime, orbit: str, cfg: Config) -> list:
    """
    Query ASF for Sentinel-1 scenes matching a given date and orbit.
    The bounding box is defined as ASF_SEARCH_BBOX in the .env

    :param start_date: The start date of the search range.
    :param end_date: The end date of the search range.
    :param orbit: The relative orbit number to match.
    :param cfg: Model configuration defined in config.py

    :return: A list of scenes matching the search criteria.
    """
    scenes = asf.search(
        platform=asf.PLATFORM.SENTINEL1,
        processingLevel=asf.PRODUCT_TYPE.GRD_HD,
        start=start_date,
        end=end_date,
        relativeOrbit=int(orbit),
        intersectsWith=cfg.asf_search_bbox,
    )

    return list(scenes)


def download_s1_scenes(scenes: list, output_dir: Path, cfg: Config) -> list:
    """
    Download a list of Sentinel-1 scenes to output_dir
    You must create an ASF Earthdata account and store the credentials in the .env file.

    :param scenes: List of ASF search results for Sentinel-1 scenes
    :param output_dir: Directory where the downloaded files should be stored.
    :param cfg: Model configuration defined in config.py

    :return: List of downloaded zip files
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    session = asf.ASFSession().auth_with_creds(
        username=cfg.asf_username,
        password=cfg.asf_password,
    )

    asf.download_urls(
        urls=[s.properties["url"] for s in scenes],
        path=str(output_dir),
        session=session,
    )

    return list(output_dir.glob("*.zip"))


def search_and_download_s1(start_date: datetime, end_date: datetime, orbit: str, cfg: Config) -> list:
    """
    Convenience wrapper that combines search and download in one call

    :param start_date: The start date of the search range.
    :param end_date: The end date of the search range.
    :param orbit: The relative orbit number to match.
    :param cfg: Model configuration defined in config.py

    :return:
    """
    scenes = search_s1_scenes(start_date, end_date, orbit, cfg)

    if not scenes:
        print(f"No Sentinel-1 scenes found between {start_date} and {end_date} in orbit {orbit}!")
        return []

    print(f"Found {len(scenes)} scene(s) between {start_date} and {end_date} in orbit {orbit}")

    return download_s1_scenes(scenes, cfg.s1_mosaic_dir, cfg)


def submit_rtc_jobs(scenes: list, work_dir: Path, cfg: Config, job_name: str = None) -> Batch:
    """
    Submit Sentinel-1 GRD scenes to ASF HyP3 for RTC_GAMMA processing.
    HyP3 performs the SNAP-equivalent processing chain described in Dunmire et al. 2024:
    orbit application, border noise removal, thermal noise removal, radiometric calibration,
    terrain flattening to gamma0, and range-Doppler terrain correction.

    Submits each scene with: radiometry=gamma0, scale=power, include_inc_map=True (for LIA),
    resolution=30 m. Existing successful jobs with the same name are reused rather than resubmitted.

    :param scenes: List of ASF search results for Sentinel-1 scenes
    :param work_dir: Directory where downloaded RTC zips will be cached
    :param cfg: Model configuration defined in config.py
    :param job_name: HyP3 job name for grouping (default derived from work_dir)

    :return: A HyP3 Batch of completed jobs
    """
    work_dir.mkdir(parents=True, exist_ok=True)
    if job_name is None:
        job_name = f"s1snow-{work_dir.name}"

    hyp3 = HyP3(username=cfg.asf_username, password=cfg.asf_password)

    existing = hyp3.find_jobs(name=job_name)
    existing_granules = {
        j.job_parameters["granules"][0]
        for j in existing
        if j.status_code in ("SUCCEEDED", "RUNNING", "PENDING")
    }

    submitted = Batch()
    for s in scenes:
        granule = s.properties["sceneName"]
        if granule in existing_granules:
            continue
        submitted += hyp3.submit_rtc_job(
            granule=granule,
            name=job_name,
            radiometry="gamma0",
            scale="power",
            resolution=30,
            include_inc_map=True,
            dem_matching=False,
            speckle_filter=False,
        )

    if len(submitted) > 0:
        print(f"Submitted {len(submitted)} new RTC job(s); waiting for completion...")
        submitted = hyp3.watch(submitted)

    all_jobs = hyp3.find_jobs(name=job_name)
    succeeded = Batch([j for j in all_jobs if j.status_code == "SUCCEEDED"])
    print(f"{len(succeeded)} RTC job(s) succeeded for '{job_name}'")
    succeeded.download_files(location=str(work_dir))

    return succeeded


def _unzip_rtc_products(work_dir: Path) -> list:
    """
    Unzip any RTC product zips in work_dir that haven't been extracted yet.

    :param work_dir: Directory containing HyP3 RTC zip files
    :return: List of extracted product directories
    """
    product_dirs = []
    for zf in work_dir.glob("*.zip"):
        out_dir = work_dir / zf.stem
        if not out_dir.exists():
            with zipfile.ZipFile(zf, "r") as z:
                z.extractall(work_dir)
        if out_dir.exists():
            product_dirs.append(out_dir)
    return product_dirs


def _filter_products_for_date_orbit(product_dirs: list, date: str, scenes: list) -> list:
    """
    Select RTC product directories whose source granule matches the given date and is in `scenes`.
    HyP3 RTC product names embed the source acquisition timestamp, e.g.
    'S1A_IW_20190301T053129_DVP_RTC30_G_gpuned_ABCD' -> matches date 20190301.

    :param product_dirs: All extracted RTC product directories
    :param date: Acquisition date (YYYYMMDD)
    :param scenes: ASF scenes that define the granules of interest for this orbit
    :return: Filtered list of product directories
    """
    granule_dates = {s.properties["sceneName"][17:25] for s in scenes}
    if date not in granule_dates:
        return []

    matching = []
    for pd in product_dirs:
        m = re.search(r"S1[AB]_IW_(\d{8})T\d{6}", pd.name)
        if m and m.group(1) == date:
            matching.append(pd)
    return matching


def _read_rtc_product(product_dir: Path) -> xr.Dataset:
    """
    Read an unzipped HyP3 RTC product into an xarray Dataset with VV and VH in linear gamma0 power, and LIA in degrees.
    We deliberately keep VV and VH in linear power so they can be safely spatially averaged when reprojecting to
    coarser grids (averaging dB values is not physically meaningful).

    :param product_dir: Path to an extracted HyP3 RTC product directory
    :return: xr.Dataset with data_vars VV (linear), VH (linear), LIA (deg) on the product's native CRS/grid
    """
    name = product_dir.name
    vv_path = product_dir / f"{name}_VV.tif"
    vh_path = product_dir / f"{name}_VH.tif"
    inc_path = product_dir / f"{name}_inc_map.tif"

    vv = rioxarray.open_rasterio(vv_path).squeeze(drop=True)
    vh = rioxarray.open_rasterio(vh_path).squeeze(drop=True)
    inc = rioxarray.open_rasterio(inc_path).squeeze(drop=True)

    vv_lin = vv.where(vv > 0).astype("float64")
    vh_lin = vh.where(vh > 0).astype("float64")
    lia_deg = np.rad2deg(inc.where(inc > 0)).astype("float64")

    ds = xr.Dataset(
        {
            "VV": vv_lin,
            "VH": vh_lin,
            "LIA": lia_deg,
        }
    )
    ds.rio.write_crs(vv.rio.crs, inplace=True)
    for v in ds.data_vars:
        ds[v].rio.write_nodata(np.nan, inplace=True)
    return ds


def build_s1_mosaic(
    date: str,
    orbit: str,
    scenes: list,
    work_dir: Path,
    cfg: Config,
    bbox: tuple = None,
    target_resolution_m: int = 1000,
) -> Path:
    """
    Build a single-date, single-orbit S1 mosaic NetCDF in the format 'S1mosaic_YYYYMMDD_ORB_PASS.nc'
    (e.g. S1mosaic_20190301_168_2.nc), where PASS is 1=ascending, 2=descending. Variables are VV, CR (=VH),
    LIA in dB / degrees on a WGS84 (EPSG:4326) grid at `target_resolution_m` (default 1 km, matching Lievens et al. 2022).

    Assumes RTC products from `submit_rtc_jobs` have already been downloaded and unzipped under `work_dir`.

    :param date: Acquisition date in YYYYMMDD format
    :param orbit: Relative orbit number (string; will be zero-padded to 3 chars)
    :param scenes: ASF scenes for this date+orbit (used to filter product dirs and read pass direction)
    :param work_dir: Directory containing the unzipped HyP3 RTC products
    :param cfg: Model configuration; output is written to cfg.s1_mosaic_dir
    :param bbox: Optional (minlon, minlat, maxlon, maxlat) to crop the mosaic
    :param target_resolution_m: Output horizontal resolution in metres (default 1000)

    :return: Path to the written mosaic NetCDF
    """
    cfg.s1_mosaic_dir.mkdir(parents=True, exist_ok=True)

    pass_dir = scenes[0].properties.get("flightDirection", "DESCENDING").upper()
    pass_code = "1" if pass_dir.startswith("A") else "2"
    orbit_str = f"{int(orbit):03d}"
    out_path = cfg.s1_mosaic_dir / f"S1mosaic_{date}_{orbit_str}_{pass_code}.nc"

    if out_path.exists():
        print(f"S1 mosaic already exists: {out_path}")
        return out_path

    product_dirs = _unzip_rtc_products(work_dir)
    matching = _filter_products_for_date_orbit(product_dirs, date, scenes)
    if not matching:
        raise FileNotFoundError(
            f"No RTC products found in {work_dir} for date={date}, orbit={orbit}. "
            f"Run submit_rtc_jobs first."
        )

    print(f"Mosaicking {len(matching)} RTC product(s) for {date} orbit {orbit_str}...")
    datasets = [_read_rtc_product(pd) for pd in matching]

    if bbox is not None:
        minlon, minlat, maxlon, maxlat = bbox
    else:
        bounds = [d.rio.bounds() for d in (
            d.rio.reproject("EPSG:4326") for d in datasets
        )]
        minlon = min(b[0] for b in bounds)
        minlat = min(b[1] for b in bounds)
        maxlon = max(b[2] for b in bounds)
        maxlat = max(b[3] for b in bounds)

    mid_lat = 0.5 * (minlat + maxlat)
    deg_per_m_lat = 1.0 / 111320.0
    target_dlat = target_resolution_m * deg_per_m_lat
    target_dlon = target_dlat

    n_lat = int(np.round((maxlat - minlat) / target_dlat))
    n_lon = int(np.round((maxlon - minlon) / target_dlon))
    out_lat = np.linspace(maxlat - target_dlat / 2, minlat + target_dlat / 2, n_lat)
    out_lon = np.linspace(minlon + target_dlon / 2, maxlon - target_dlon / 2, n_lon)

    template = xr.DataArray(
        np.zeros((n_lat, n_lon), dtype="float64"),
        coords={"y": out_lat, "x": out_lon},
        dims=("y", "x"),
    )
    template.rio.write_crs("EPSG:4326", inplace=True)

    reprojected = []
    for ds in datasets:
        r = ds.rio.reproject_match(template, resampling=Resampling.average, nodata=np.nan)
        reprojected.append(r)

    stack_vv = xr.concat([r["VV"] for r in reprojected], dim="scene")
    stack_vh = xr.concat([r["VH"] for r in reprojected], dim="scene")
    stack_lia = xr.concat([r["LIA"] for r in reprojected], dim="scene")
    coarse_vv = stack_vv.mean(dim="scene", skipna=True)
    coarse_vh = stack_vh.mean(dim="scene", skipna=True)
    coarse_lia = stack_lia.mean(dim="scene", skipna=True)

    coarse_vv = coarse_vv.rename({"y": "lat", "x": "lon"})
    coarse_vh = coarse_vh.rename({"y": "lat", "x": "lon"})
    coarse_lia = coarse_lia.rename({"y": "lat", "x": "lon"})

    vv_db = 10.0 * np.log10(coarse_vv.where(coarse_vv > 0))
    vh_db = 10.0 * np.log10(coarse_vh.where(coarse_vh > 0))
    cr_db = vh_db - vv_db

    out = xr.Dataset(
        {
            "VV": vv_db,
            "CR": cr_db,
            "LIA": coarse_lia,
        }
    )
    out = out.drop_vars("spatial_ref", errors="ignore")

    out.to_netcdf(out_path)
    print(f"Wrote S1 mosaic: {out_path}")
    return out_path


def create_s1_mosaic(
    date: str,
    orbit: str,
    cfg: Config,
    work_dir: Path = None,
    bbox: tuple = None,
    search_window_days: int = 1,
) -> Path:
    """
    End-to-end: search ASF for S1 GRDs on `date` in `orbit`, submit RTC_GAMMA jobs to HyP3, download/unzip results,
    and build the 'S1mosaic_YYYYMMDD_ORB_PASS.nc' file under cfg.s1_mosaic_dir.

    :param date: Target acquisition date in YYYYMMDD format
    :param orbit: Relative orbit number (string)
    :param cfg: Model configuration defined in config.py
    :param work_dir: Directory for cached RTC zips/products (default cfg.s1_mosaic_dir / 'rtc_cache')
    :param bbox: Optional (minlon, minlat, maxlon, maxlat) to crop the mosaic to a sub-area
    :param search_window_days: Half-width of the date search window (default 1 day on each side)

    :return: Path to the written mosaic NetCDF
    """
    if work_dir is None:
        work_dir = cfg.s1_mosaic_dir / "rtc_cache"

    date_dt = datetime.strptime(date, "%Y%m%d")
    start = date_dt - timedelta(days=search_window_days)
    end = date_dt + timedelta(days=search_window_days)

    scenes = search_s1_scenes(start, end, orbit, cfg)
    if not scenes:
        raise RuntimeError(
            f"No Sentinel-1 scenes found around {date} for orbit {orbit} within the configured bbox"
        )
    scenes = [s for s in scenes if s.properties["sceneName"][17:25] == date]
    if not scenes:
        raise RuntimeError(f"No Sentinel-1 scenes found on exactly {date} for orbit {orbit}")

    print(f"Found {len(scenes)} scene(s) on {date} orbit {orbit}; submitting to HyP3...")
    submit_rtc_jobs(scenes, work_dir, cfg, job_name=f"s1snow-{date}-{int(orbit):03d}")

    return build_s1_mosaic(date, orbit, scenes, work_dir, cfg, bbox=bbox)