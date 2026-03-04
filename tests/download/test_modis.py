from pathlib import Path

from s1_snowdepth.config import Config
from s1_snowdepth.download.modis import download_modis

def test_download_modis():
    cfg = Config()

    download_path = Path(__file__).parent.parent / "data" / "modis"
    result = download_modis("20190301", download_path, cfg)

    assert result.exists()

if __name__ == "__main__":
    test_download_modis()
