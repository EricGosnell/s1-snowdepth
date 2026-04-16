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
@click.argument("output_dir")
def download_static_vars(output_dir):
    """Download static variables to OUTPUT_DIR."""
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
    click.echo(f"Static variables saved to {output_dir}. Set the path in your .env accordingly.")

@main.command()
@click.argument("output_dir", default=".", required=False)
def download_model(output_dir):
    """Download final_model_xg.pkl to OUTPUT_DIR."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    click.echo("Downloading final_model_xg.pkl...")
    with importlib.resources.files("s1_snowdepth").joinpath("final_model_xg.pkl") as pkl_src:
        shutil.copy(str(pkl_src), output_dir / "final_model_xg.pkl")
    click.echo(f"final_model_xg.pkl downloaded to {output_dir}/final_model_xg.pkl. Set the path in your .env accordingly.")

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