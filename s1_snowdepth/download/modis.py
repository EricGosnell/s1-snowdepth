import ee
import requests
from pathlib import Path
from datetime import datetime, timedelta

from s1_snowdepth.config import Config

def download_modis(date: str, output_dir: Path, cfg: Config) -> Path:
    """
    Download MODIS MOD10A1 fractional snow cover for a given date using Google Earth Engine.
    TODO: docs about .env...
    :param date:
    :param output_dir:
    :param cfg:
    :return:
    """
    # Initialize Google Earth Engine project
    ee.Initialize(project=cfg.gee_project)

    date_dt = datetime.strptime(date, "%Y%m%d")
    next_day = date_dt + timedelta(days=1)
    region = ee.Geometry.Rectangle(cfg.gee_search_bbox)

    modis = (
        ee.ImageCollection("MODIS/061/MOD10A1")
        .filterDate(date_dt.strftime("%Y-%m-%d"), next_day.strftime("%Y-%m-%d"))
        .filterBounds(region)
        .select("NDSI_Snow_Cover")
        .first()
    )

    # Mask invalid values (>100 are cloud/fill flags), scale to 0-1
    modis_scaled = modis.updateMask(modis.lte(100)).divide(100)

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"modis_scf_{date}.tif"

    if output_path.exists():
        print(f"MODIS file already exists: {output_path}")
        return output_path

    print(f"Downloading MODIS for {date}...")
    url = modis_scaled.getDownloadURL({
        "scale": 500,
        "region": region,
        "format": "GEO_TIFF",
        "crs": "EPSG:4326",
    })

    response = requests.get(url)
    response.raise_for_status()
    output_path.write_bytes(response.content)

    print(f"Saved to: {output_path}. File size: {output_path.stat().st_size / 1e6:.1f} MB")
    return output_path