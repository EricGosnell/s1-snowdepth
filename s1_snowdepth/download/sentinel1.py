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
