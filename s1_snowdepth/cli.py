import click
import subprocess
import shutil
from importlib.resources import as_file, files

from s1_snowdepth.config import Config
from s1_snowdepth.run.run_model_script import run as run_model
from s1_snowdepth.preprocessing.s1_scaling import create_s1_scaling
from s1_snowdepth.download.sentinel1 import create_s1_mosaic
from s1_snowdepth.preprocessing.snow_cover_fraction import compute_cumulative_scf

@click.group()
def main():
    pass

@main.command()
def init():
    """Copy .env.example to .env in the current directory."""
    env_dest = Path.cwd() / ".env"
    if env_dest.exists():
        raise click.ClickException(".env already exists in current directory.")
    with as_file(files("s1_snowdepth").joinpath(".env.example")) as env_src:
        shutil.copy(str(env_src), str(env_dest))
    click.echo(f".env created at {env_dest}. Please fill in the required values.")

@main.command()
def env_create():
    """Create the conda environment for running the model."""
    if shutil.which("conda") is None:
        raise click.ClickException(
            "conda is not installed or not on your PATH. "
            "Please install Miniconda or Anaconda first: "
            "https://docs.conda.io/en/latest/miniconda.html"
        )
    with as_file(files("s1_snowdepth").joinpath("environment.yml")) as env_path:
        subprocess.run(["conda", "env", "create", "-f", str(env_path)], check=True)

    click.echo(
        "\nConda environment created. To run the model:\n"
        "  deactivate\n"
        "  conda activate s1-snowdepth\n"
        "  s1-snowdepth run"
    )

@main.command()
@click.argument("output_dir", default=".", required=False)
def download_model(output_dir):
    """Download final_model_xg.pkl to OUTPUT_DIR (default: current directory)."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    click.echo("Downloading final_model_xg.pkl...")
    with as_file(files("s1_snowdepth").joinpath("final_model_xg.pkl")) as pkl_src:
        shutil.copy(str(pkl_src), output_dir / "final_model_xg.pkl")
    click.echo(f"final_model_xg.pkl downloaded to {output_dir}/final_model_xg.pkl.")
    click.echo(f"Update your .env to point to the model file:\n"
               f"  MODEL_PATH={output_dir}\n"
               f"  MODEL_VERSION=final_model_xg\n")

@main.command()
@click.argument("output_dir", default="static_vars", required=False)
def download_static_vars(output_dir):
    """Download static variables to OUTPUT_DIR (default: static_vars)."""
    try:
        from huggingface_hub import snapshot_download
    except ImportError:
        raise click.ClickException(
            "huggingface_hub is required to download static variables. "
            "Install it with: pip install huggingface-hub"
        )
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    click.echo("Downloading static variables...")
    snapshot_download(
        repo_id="EricGosnell/S1-Snowdepth-Static-Variables",
        repo_type="dataset",
        local_dir=str(output_dir),
    )
    click.echo(f"Static variables saved to {output_dir}.")
    click.echo(f"Update your .env to point to the static variables:\n"
               f"  STATIC_VAR_PATH={output_dir}/all_static_var.nc\n"
               f"  SNOW_CLASS_PATH={output_dir}/snowclass.nc\n"
               f"  LANDCOVER_PATH={output_dir}/landcover_1.nc\n"
               f"  GRID_PATH={output_dir}/grid_lowres.nc\n"
               f"  GLACIER_PATH={output_dir}/glacier_raster.nc\n")


@main.command()
@click.argument("output_dir", default=".", required=False)
def download_sample_data(output_dir):
    """Download sample input files for testing the model to OUTPUT_DIR (default: current directory)."""
    try:
        from huggingface_hub import snapshot_download
    except ImportError:
        raise click.ClickException(
            "huggingface_hub is required to download sample data. "
            "Install it with: pip install huggingface-hub"
        )
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    click.echo("Downloading sample data...")
    snapshot_download(
        repo_id="EricGosnell/S1-Snowdepth-Sample-Data",
        repo_type="dataset",
        local_dir=str(output_dir),
    )
    click.echo(f"Sample data saved to {output_dir}.")
    click.echo(
        "Update your .env to point to the sample data:\n"
        f"  S1_MOSAIC_DIR={output_dir}/s1_mosaic/\n"
        f"  S1_SCALING_DIR={output_dir}/s1_scaling/\n"
        f"  SNOW_COVER_DIR={output_dir}/snow_cover/\n"
    )

@main.command(name="build-scaling")
@click.option("--orbit", required=True, help="Relative orbit number, e.g. 168")
@click.option("--year", required=True, type=int, help="Snow year (the calendar year of the Aug-Dec baseline window)")
@click.option("--bbox", default=None,
              help="Optional 'minlon,minlat,maxlon,maxlat' to crop the scaling grid. Defaults to GEE_SEARCH_BBOX.")
@click.option("--keep-rtc-cache", is_flag=True, default=False,
              help="Keep all per-date HyP3 RTC zips/products after each daily mosaic is built. "
                   "Default cleans them up to save disk (~20 GB per date).")
def build_scaling(orbit, year, bbox, keep_rtc_cache):
    """Build the S1_YYYY_OOO_scale.nc file for a given orbit and snow year by submitting
    Aug-Dec acquisitions to ASF HyP3, mosaicking each, and computing the per-pixel 25th percentile."""
    cfg = Config()
    bbox_tuple = None
    if bbox:
        bbox_tuple = tuple(float(x) for x in bbox.split(","))
        if len(bbox_tuple) != 4:
            raise click.ClickException("--bbox must be 'minlon,minlat,maxlon,maxlat'")
    out_path = create_s1_scaling(year=year, orbit=orbit, cfg=cfg, bbox=bbox_tuple, keep_rtc_cache=keep_rtc_cache)
    click.echo(f"Built scaling file: {out_path}")

@main.command(name="create-s1-mosaic")
@click.option("--date", required=True, help="Acquisition date in YYYYMMDD format")
@click.option("--orbit", required=True, help="Relative orbit number, e.g. 168")
@click.option("--bbox", default=None,
              help="Optional 'minlon,minlat,maxlon,maxlat' to crop the mosaic. Defaults to the full S1 footprint.")
def create_s1_mosaic_cmd(date, orbit, bbox):
    """Download Sentinel-1 GRDs for a given date and orbit, submit RTC processing to ASF HyP3,
    and build the S1mosaic_YYYYMMDD_OOO_PASS.nc file under S1_MOSAIC_DIR."""
    cfg = Config()
    bbox_tuple = None
    if bbox:
        bbox_tuple = tuple(float(x) for x in bbox.split(","))
        if len(bbox_tuple) != 4:
            raise click.ClickException("--bbox must be 'minlon,minlat,maxlon,maxlat'")
    out_path = create_s1_mosaic(date=date, orbit=orbit, cfg=cfg, bbox=bbox_tuple)
    click.echo(f"Built S1 mosaic: {out_path}")

@main.command(name="create-snowcover")
@click.argument("date")
def create_snowcover(date):
    """Build the snowcover_YYYYMMDD_.nc file by averaging IMS binary snow cover with MODIS MOD10A1
    fractional snow cover (5-day gap-filled). Region is set by GEE_SEARCH_BBOX in your .env."""
    cfg = Config()
    cfg.snow_cover_dir.mkdir(parents=True, exist_ok=True)
    ims_dir = cfg.snow_cover_dir / "ims"
    modis_dir = cfg.snow_cover_dir / "modis"
    out_path = cfg.snow_cover_dir / f"snowcover_{date}_.nc"
    if out_path.exists():
        click.echo(f"Snow cover file already exists: {out_path}")
        return
    ds = compute_cumulative_scf(date, ims_dir, modis_dir, cfg)
    ds.to_netcdf(out_path)
    click.echo(f"Built snow cover file: {out_path}")

@main.command()
def run():
    """Run the snow depth estimation model over all S1 mosaic files."""
    import sys
    if sys.version_info >= (3, 12):
        raise click.ClickException(
            "The model requires Python 3.9-3.11 (pycaret limitation). "
            "Please activate the conda environment first:\n"
            "  conda activate s1-snowdepth"
        )
    cfg = Config()
    run_model(cfg)
    