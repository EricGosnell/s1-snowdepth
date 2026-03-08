import gzip
import shutil
import requests
from pathlib import Path
from datetime import datetime

from s1_snowdepth.config import Config

def download_ims(date: str, output_dir: Path, cfg: Config) -> Path:
    """
    Download IMS daily 1km binary snow cover data for a given date
    Files are hosted on NSIDC as gzipped NetCDF files and decompressed on download. No credentials required.

    :param date: Date to download IMS data for (YYYYMMDD format)
    :param output_dir: Directory where the downloaded files should be stored.
    :param cfg: Model configuration defined in config.py and set in .env
    :return: Path to the downloaded file
    """
    date_dt = datetime.strptime(date, "%Y%m%d")
    year = date_dt.year
    doy = date_dt.strftime("%j")  # day of year, zero padded to 3 digits

    filename = f"ims{year}{doy}_1km_v1.3.nc.gz"
    url = f"https://noaadata.apps.nsidc.org/NOAA/G02156/netcdf/1km/{year}/{filename}"

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"ims_{date}_1km.nc"

    if output_path.exists():
        print(f"IMS file already exists: {output_path}")
        return output_path

    print(f"Downloading IMS for {date} from {url}...")
    response = requests.get(url)
    response.raise_for_status()

    # Write compressed file, decompress, then remove the .gz
    gz_path = output_dir / filename
    gz_path.write_bytes(response.content)

    with gzip.open(gz_path, 'rb') as f_in:
        with open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    gz_path.unlink()

    print(f"Saved to: {output_path}. File size: {output_path.stat().st_size / 1e6:.1f} MB")
    return output_path