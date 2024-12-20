import click
import subprocess
import sys
from pathlib import Path


@click.group()
def cli():
    """Library Organizer CLI"""
    pass


@cli.command()
def install():
    """Install production dependencies"""
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "-r", "requirements.txt"]
    )


@cli.command()
def dev_install():
    """Install development dependencies"""
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "-r", "requirements.txt"]
    )
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "-r", "requirements-dev.txt"]
    )


@cli.command()
@click.option("--config", type=Path, default="config.yaml", help="Path to config file")
@click.option("--dry-run", is_flag=True, help="Run without making changes")
def run(config, dry_run):
    """Run the library organizer"""
    cmd = [sys.executable, "scripts/run_analysis.py"]
    if config:
        cmd.extend(["--config", str(config)])
    if dry_run:
        cmd.append("--dry-run")
    subprocess.check_call(cmd)


@cli.command()
def test():
    """Run tests"""
    subprocess.check_call([sys.executable, "-m", "pytest", "tests/", "-v"])


@cli.command()
def lint():
    """Run linters"""
    subprocess.check_call(["flake8", "src/", "tests/"])
    subprocess.check_call(["black", "--check", "src/", "tests/"])


@cli.command()
def format():
    """Format code with black"""
    subprocess.check_call(["black", "src/", "tests/"])


if __name__ == "__main__":
    cli()
