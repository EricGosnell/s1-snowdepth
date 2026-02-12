from pathlib import Path
import os
from dotenv import load_dotenv, find_dotenv

class Config:
    """
    TODO: docs
    """

    def __init__ (self,
          # Input configs
          s1_mosaic_dir: str = None,
          s1_scaling_dir: str = None,
          snow_cover_dir: str = None,
          static_var_path: str = None,
          snow_class_path: str = None,
          landcover_path: str = None,
          grid_path: str = None,
          glacier_path: str = None,
          model_path: str = None,
          model_version: str = "final_model_xg",

          # Output configs
          output_dir: str = None,

          # Runtime configs
          skip_orbits: tuple = ("051", "037"),
          skip_months: tuple = ("06", "07", "08"),
    ):
        # Load .env file into os.environment
        load_dotenv(find_dotenv(usecwd=True))

        # Input configs
        self.s1_mosaic_dir = Path(
            s1_mosaic_dir
            or os.environ.get("S1_MOSAIC_DIR","")
        )
        self.s1_scaling_dir = Path(
            s1_scaling_dir
            or os.environ.get("S1_SCALING_DIR","")
        )
        self.snow_cover_dir = Path(
            snow_cover_dir
            or os.environ.get("SNOW_COVER_DIR","")
        )
        self.static_var_path = Path(
            static_var_path
            or os.environ.get("STATIC_VAR_PATH","")
        )
        self.snow_class_path = Path(
            snow_class_path
            or os.environ.get("SNOW_CLASS_PATH","")
        )
        self.landcover_path = Path(
            landcover_path
            or os.environ.get("LANDCOVER_PATH","")
        )
        self.grid_path = Path(
            grid_path
            or os.environ.get("GRID_PATH","")
        )
        self.glacier_path = Path(
            glacier_path
            or os.environ.get("GLACIER_PATH","")
        )
        self.model_path = Path(
            model_path
            or os.environ.get("MODEL_PATH","")
        )
        self.model_version = model_version

        # Output configs
        self.output_dir = Path(
            output_dir
            or os.environ.get("OUTPUT_DIR","")
        )

        # Runtime configs
        self.skip_orbits = skip_orbits
        self.skip_months = skip_months