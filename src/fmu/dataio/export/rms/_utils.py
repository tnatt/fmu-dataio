from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

from packaging.version import parse as versionparse

from fmu.config.utilities import yaml_load
from fmu.dataio._logging import null_logger

from ._conditional_rms_imports import import_rms_package

if TYPE_CHECKING:
    from packaging.version import Version


logger: Final = null_logger(__name__)


rmsapi, _ = import_rms_package()

RMS_API_PROJECT_MAPPING = {
    "1.7": "13.1",
}


def _get_rmsapi_version() -> Version:
    """Get the rmsapi version"""
    return versionparse(rmsapi.__version__)


def _check_rmsapi_version(minimum_version: str) -> None:
    """Check if we are working in a RMS API, and also check RMS versions?"""
    logger.debug("Check API version...")
    if _get_rmsapi_version() < versionparse(minimum_version):
        raise RuntimeError(
            f"You need at least API version {minimum_version} "
            f"(RMS {RMS_API_PROJECT_MAPPING}) to use this function."
        )
    logger.debug("Check API version... DONE")


def _get_rms_project_units(project: Any) -> str:
    """See if the RMS project is defined in metric or feet."""

    units = project.project_units
    logger.debug("Units are %s", units)
    return str(units)


def _load_config(config_path: Path) -> dict[str, Any]:
    """Set the global config data by reading the file."""
    logger.debug("Set global config...")

    if not isinstance(config_path, Path):
        raise ValueError("The config_path argument needs to be a path")

    if not config_path.is_file():
        raise FileNotFoundError(f"Cannot find file for global config: {config_path}")

    return yaml_load(config_path)