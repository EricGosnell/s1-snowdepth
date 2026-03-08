from pathlib import Path

from s1_snowdepth.config import Config
from s1_snowdepth.download.ims import download_ims

def test_download_ims():
    cfg = Config()

    download_dir = Path(__file__).parent.parent / "data" / "ims"
    result = download_ims("20190301", download_dir, cfg)

    assert result.exists()

if __name__ == "__main__":
    test_download_ims()