import click
from s1_snowdepth.config import Config
from s1_snowdepth.run.run_model_script import run as run_model

@click.group()
def main():
    pass

@main.command()
def run():
    """Run the snow depth estimation model over all S1 mosaic files."""
    cfg = Config()
    run_model(cfg)