from datetime import datetime, timedelta
from pathlib import Path

from s1_snowdepth.config import Config
from s1_snowdepth.download.sentinel1 import search_s1_scenes

def test_search_s1_scenes():
    TEST_BBOX = "POLYGON((11.0 47.0, 11.5 47.0, 11.5 47.5, 11.0 47.5, 11.0 47.0))"

    end = datetime.now()
    start = end - timedelta(days=6)

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


if __name__ == "__main__":
    test_search_s1_scenes()