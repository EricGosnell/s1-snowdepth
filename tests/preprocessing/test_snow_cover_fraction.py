from pathlib import Path

from s1_snowdepth.config import Config
from s1_snowdepth.preprocessing.snow_cover_fraction import load_ims
from s1_snowdepth.download.ims import download_ims

def test_load_ims():
    cfg = Config()

    date = "20190301"
    ims_dir = Path(__file__).parent.parent / "data" / "ims"
    ims_path = ims_dir / f"ims_{date}_1km.nc"

    if not ims_path.exists():
        print(f"No IMS file found, downloading...")
        download_ims(date, ims_dir, cfg)

    ims = load_ims(ims_path, cfg)

    # Convert to tif
    # ims.rio.to_raster(ims_path.with_suffix(".tif"))

    n_snow_pixels = int(ims.sum())
    print(f"IMS shape: {ims.shape}\n"
          f"    CRS: {ims.rio.crs}\n"
          f"    snow pixels: {n_snow_pixels}\n"
          f"    dims: {ims.dims}")

    assert (n_snow_pixels > 0)

if __name__ == "__main__":
    test_load_ims()