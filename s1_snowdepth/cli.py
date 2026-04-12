import click
import subprocess
import shutil
import importlib.resources
from pathlib import Path

from s1_snowdepth.config import Config
from s1_snowdepth.run.run_model_script import run as run_model

@click.group()
def main():
    pass

@main.command()
def init():
    """Copy .env.example to .env in the current directory."""
    env_dest = Path.cwd() / ".env"
    if env_dest.exists():
        raise click.ClickException(".env already exists in current directory.")
    with importlib.resources.files("s1_snowdepth").joinpath(".env.example") as env_src:
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
    with importlib.resources.files("s1_snowdepth").joinpath("environment.yml") as env_path:
        subprocess.run(["conda", "env", "create", "-f", str(env_path)], check=True)

    click.echo(
        "\nConda environment created. To run the model:\n"
        "  deactivate\n"
        "  conda activate s1-snowdepth\n"
        "  s1-snowdepth run"
    )

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