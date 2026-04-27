from pathlib import Path
from datetime import datetime
import numpy as np
import xarray as xr

from s1_snowdepth.config import Config
from s1_snowdepth.download.sentinel1 import search_s1_scenes, submit_rtc_jobs, build_s1_mosaic


def _baseline_dates(year: int, orbit: str, cfg: Config) -> list:
    """
    Find all S1 scenes for the given orbit between Aug 1 and Dec 31 of the snow year
    (Dunmire et al., 2024 §2.1: 25th percentile is computed over this no-snow window).

    :param year: Snow year (the calendar year that contains the Aug 1 start)
    :param orbit: Relative orbit number (string)
    :param cfg: Model configuration; uses cfg.asf_search_bbox
    :return: List of ASF scene results in the baseline window
    """
    start = datetime(year, 8, 1)
    end = datetime(year, 12, 31, 23, 59, 59)
    return search_s1_scenes(start, end, orbit, cfg)


def create_s1_scaling(
    year: int,
    orbit: str,
    cfg: Config,
    work_dir: Path = None,
    bbox: tuple = None,
    target_resolution_m: int = 1000,
    quantile: float = 0.25,
) -> Path:
    """
    Build the per-orbit per-snow-year scaling NetCDF in the format 'S1_YYYY_OOO_scale.nc' (e.g. S1_2018_168_scale.nc)
    under cfg.s1_scaling_dir.

    Following Dunmire et al. 2024 §2.1, this is the 25th-percentile per-pixel backscatter / cross-pol-ratio across all
    S1 acquisitions for the given relative orbit between 1 August and 31 December of the snow year, used to normalize
    individual scenes to no-snow conditions.

    Variables in the output match the convention of the reference scaling files:
      g0vv : q25 of VV in dB
      g0vh : q25 of raw VH in dB
      lia  : q25 of local incidence angle in degrees
      cr   : q25 of (VH - VV) in dB

    :param year: Snow year (the calendar year of the Aug-Dec baseline window)
    :param orbit: Relative orbit number (string; will be zero-padded to 3 chars)
    :param cfg: Model configuration; output is written to cfg.s1_scaling_dir
    :param work_dir: Directory for cached HyP3 RTC products (default cfg.s1_scaling_dir / 'rtc_cache')
    :param bbox: Optional (minlon, minlat, maxlon, maxlat) to crop the scaling grid
    :param target_resolution_m: Output horizontal resolution in metres (default 1000)
    :param quantile: Percentile to use for scaling (default 0.25)

    :return: Path to the written scaling NetCDF
    """
    cfg.s1_scaling_dir.mkdir(parents=True, exist_ok=True)
    if work_dir is None:
        work_dir = cfg.s1_scaling_dir / "rtc_cache"
    work_dir.mkdir(parents=True, exist_ok=True)

    orbit_str = f"{int(orbit):03d}"
    out_path = cfg.s1_scaling_dir / f"S1_{year}_{orbit_str}_scale.nc"
    if out_path.exists():
        print(f"S1 scaling file already exists: {out_path}")
        return out_path

    all_scenes = _baseline_dates(year, orbit, cfg)
    if not all_scenes:
        raise RuntimeError(
            f"No Sentinel-1 scenes found between {year}-08-01 and {year}-12-31 "
            f"for orbit {orbit_str} within the configured bbox"
        )

    scenes_by_date = {}
    for s in all_scenes:
        d = s.properties["sceneName"][17:25]
        scenes_by_date.setdefault(d, []).append(s)
    dates = sorted(scenes_by_date.keys())
    print(f"Found {len(all_scenes)} scene(s) on {len(dates)} acquisition date(s) for orbit {orbit_str}: {dates[0]}..{dates[-1]}")

    daily_mosaics = []
    for date in dates:
        scenes = scenes_by_date[date]
        print(f"  [{date}] Submitting {len(scenes)} scene(s) to HyP3...")
        submit_rtc_jobs(scenes, work_dir, cfg, job_name=f"s1snow-{date}-{orbit_str}")

        try:
            mosaic_path = build_s1_mosaic(
                date=date,
                orbit=orbit_str,
                scenes=scenes,
                work_dir=work_dir,
                cfg=cfg,
                bbox=bbox,
                target_resolution_m=target_resolution_m,
            )
        except FileNotFoundError as e:
            print(f"  [{date}] WARNING: could not build mosaic: {e}; skipping")
            continue

        ds = xr.open_dataset(mosaic_path).expand_dims(time=[np.datetime64(f"{date[0:4]}-{date[4:6]}-{date[6:8]}")])
        daily_mosaics.append(ds)

    if not daily_mosaics:
        raise RuntimeError(f"No daily mosaics could be built for orbit {orbit_str} in {year}; cannot compute scaling")

    print(f"Computing q{int(quantile * 100)} across {len(daily_mosaics)} daily mosaic(s)...")
    stack = xr.concat(daily_mosaics, dim="time")

    g0vv = stack["VV"].quantile(quantile, dim="time", skipna=True)
    cr = stack["CR"].quantile(quantile, dim="time", skipna=True)
    lia = stack["LIA"].quantile(quantile, dim="time", skipna=True)
    g0vh = (stack["VV"] + stack["CR"]).quantile(quantile, dim="time", skipna=True)

    out = xr.Dataset(
        {
            "g0vv": g0vv.astype("float32"),
            "g0vh": g0vh.astype("float32"),
            "lia": lia.astype("float32"),
            "cr": cr.astype("float32"),
        }
    )
    out = out.drop_vars("spatial_ref", errors="ignore")
    out.to_netcdf(out_path)
    print(f"Wrote S1 scaling file: {out_path}")
    return out_path