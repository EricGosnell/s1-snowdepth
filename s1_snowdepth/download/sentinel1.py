import asf_search as asf
from pathlib import Path
from datetime import datetime

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