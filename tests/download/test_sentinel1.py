from datetime import datetime, timedelta
from pathlib import Path

from s1_snowdepth.config import Config
from s1_snowdepth.download.sentinel1 import search_s1_scenes, download_s1_scenes, create_s1_mosaic

def test_search_s1_scenes():
    TEST_BBOX = "POLYGON((11.0 47.0, 11.2 47.0, 11.2 47.2, 11.0 47.2, 11.0 47.0))"

    end = datetime.now()
    start = end - timedelta(days=6) # Orbits repeat at least every 6 days

    cfg = Config(asf_search_bbox=TEST_BBOX)

    scenes = search_s1_scenes(start_date=start, end_date=end, orbit="117", cfg=cfg)

    print(f"Found {len(scenes)} scenes")
    for s in scenes:
        print(
            s.properties["sceneName"],
            s.properties["pathNumber"],
            s.properties["startTime"],
        )

    assert(len(scenes) > 0)

def test_download_s1_scenes():
    TEST_BBOX = "POLYGON((11.0 47.0, 11.2 47.0, 11.2 47.2, 11.0 47.2, 11.0 47.0))"

    end = datetime.now()
    start = end - timedelta(days=6) # Orbits repeat at least every 6 days

    cfg = Config(asf_search_bbox=TEST_BBOX)
    print(f"ASF Username: {cfg.asf_username}")

    scenes = search_s1_scenes(start_date=start, end_date=end, orbit="117", cfg=cfg)
    print(f"Found {len(scenes)} scenes")
    for s in scenes:
        print(
            s.properties["sceneName"],
            s.properties["pathNumber"],
            s.properties["startTime"],
        )

    download_dir = Path(__file__).parent.parent / "data" / "s1_mosaic"
    downloaded_files = download_s1_scenes(scenes, download_dir, cfg)

    print(downloaded_files)
    assert len(downloaded_files) > 0

def test_create_s1_mosaic():
    TEST_BBOX_WKT = "POLYGON((10.481 46.671, 11.424 46.671, 11.424 47.614, 10.481 47.614, 10.481 46.671))"
    TEST_BBOX = (10.481, 46.671, 11.424, 47.614)
    TARGET_DATE = "20190301"
    TARGET_ORBIT = "168"

    cfg = Config(asf_search_bbox=TEST_BBOX_WKT)

    print(f"ASF Username: {cfg.asf_username}")
    print(f"Building mosaic for date={TARGET_DATE}, orbit={TARGET_ORBIT}")
    mosaic_path = create_s1_mosaic(
        date=TARGET_DATE,
        orbit=TARGET_ORBIT,
        cfg=cfg,
        bbox=TEST_BBOX,
    )
    print(f"Wrote mosaic: {mosaic_path}")

    assert mosaic_path.exists()
    assert mosaic_path.name.startswith(f"S1mosaic_{TARGET_DATE}_{TARGET_ORBIT}_")
    assert mosaic_path.suffix == ".nc"

    import xarray as xr
    ds = xr.open_dataset(mosaic_path)
    print(ds)
    assert "VV" in ds
    assert "CR" in ds
    assert "LIA" in ds
    ds.close()

if __name__ == "__main__":
    # test_search_s1_scenes()
    # test_download_s1_scenes()
    test_create_s1_mosaic()