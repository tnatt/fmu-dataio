from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

import pandas as pd
from packaging.version import parse as versionparse

import fmu.dataio as dio
from fmu.config.utilities import yaml_load
from fmu.dataio._logging import null_logger

from .._decorators import experimental
from .._export_result import ExportResult, ExportResultItem
from ._conditional_rms_imports import import_rms_package

_modules = import_rms_package()
if _modules:
    rmsapi = _modules["rmsapi"]
    jobs = _modules["jobs"]


_logger: Final = null_logger(__name__)

# rename columns to FMU standard
_RENAME_COLUMNS_FROM_RMS: Final = {
    "Proj. real.": "REAL",
    "Zone": "ZONE",
    "Segment": "REGION",
    "Boundary": "LICENSE",
    "Facies": "FACIES",
    "BulkOil": "BULK_OIL",
    "NetOil": "NET_OIL",
    "PoreOil": "PORV_OIL",
    "HCPVOil": "HCPV_OIL",
    "STOIIP": "STOIIP_OIL",
    "AssociatedGas": "ASSOCIATEDGAS_OIL",
    "BulkGas": "BULK_GAS",
    "NetGas": "NET_GAS",
    "PoreGas": "PORV_GAS",
    "HCPVGas": "HCPV_GAS",
    "GIIP": "GIIP_GAS",
    "AssociatedLiquid": "ASSOCIATEDOIL_GAS",
    "Bulk": "BULK_TOTAL",
    "Net": "NET_TOTAL",
    "Pore": "PORV_TOTAL",
}


@dataclass
class _ExportVolumetricsRMS:
    project: Any
    grid_name: str
    volume_job_name: str

    # optional and defaulted
    config_path: str | Path = "../../fmuconfig/output/global_variables.yml"
    classification: str | None = None

    # internal storage instance variables
    _config: dict = field(default_factory=dict, init=False)
    _volume_job: dict = field(default_factory=dict, init=False)
    _volume_table_name: str = field(default="", init=False)
    _dataframe: pd.DataFrame = field(default_factory=pd.DataFrame, init=False)
    _units: str = field(default="metric", init=False)

    def __post_init__(self) -> None:
        _logger.debug("Process data, estiblish state prior to export.")
        self._check_rmsapi_version()
        self._set_config()
        self._rms_volume_job_settings()
        self._read_volume_table_name_from_rms()
        self._voltable_as_dataframe()
        self._set_units()
        _logger.debug("Process data... DONE")

    @staticmethod
    def _check_rmsapi_version() -> None:
        """Check if we are working in a RMS API, and also check RMS versions?"""
        _logger.debug("Check API version...")
        if versionparse(rmsapi.__version__) < versionparse("1.7"):
            raise RuntimeError(
                "You need at least API version 1.7 (RMS 13.1) to use this function."
            )
        _logger.debug("Check API version... DONE")

    def _set_config(self) -> None:
        """Set the global config data by reading the file."""
        _logger.debug("Set global config...")

        if isinstance(self.config_path, dict):
            raise ValueError("The config_path argument needs to be a string or a path")

        global_config_path = Path(self.config_path)

        if not global_config_path.is_file():
            raise FileNotFoundError(
                f"Cannot find file for global config: {self.config_path}"
            )
        self._config = yaml_load(global_config_path)
        _logger.debug("Read config from yaml... DONE")

    def _rms_volume_job_settings(self) -> None:
        """Get information out from the RMS job API."""
        _logger.debug("RMS VOLJOB settings...")
        self._volume_job = jobs.Job.get_job(
            owner=["Grid models", self.grid_name, "Grid"],
            type="Volumetrics",
            name=self.volume_job_name,
        ).get_arguments()
        _logger.debug("RMS VOLJOB settings... DONE")

    def _read_volume_table_name_from_rms(self) -> None:
        """Read the volume table name from RMS."""
        _logger.debug("Read volume table name from RMS...")
        voltable = self._volume_job.get("Report")
        if isinstance(voltable, list):
            voltable = voltable[0]
        self._volume_table_name = voltable.get("ReportTableName")

        if not self._volume_table_name:
            raise RuntimeError(
                "You need to configure output to Report file: Report table "
                "in the volumetric job. Provide a table name and rerun the job."
            )

        _logger.debug("The volume table name is %s", self._volume_table_name)
        _logger.debug("Read volume table name from RMS... DONE")

    def _voltable_as_dataframe(self) -> None:
        """Convert table to pandas dataframe"""
        _logger.debug("Read values and convert to pandas dataframe...")
        dict_values = (
            self.project.volumetric_tables[self._volume_table_name]
            .get_data_table()
            .to_dict()
        )
        _logger.debug("Dict values are: %s", dict_values)
        self._dataframe = pd.DataFrame.from_dict(dict_values)
        self._dataframe.rename(columns=_RENAME_COLUMNS_FROM_RMS, inplace=True)
        self._dataframe.drop("REAL", axis=1, inplace=True, errors="ignore")

        _logger.debug("Read values and convert to pandas dataframe... DONE")

    def _set_units(self) -> None:
        """See if the RMS project is defined in metric or feet."""

        units = self.project.project_units
        _logger.debug("Units are %s", units)
        self._units = str(units)

    def _export_volume_table(self) -> ExportResult:
        """Do the actual volume table export using dataio setup."""

        edata = dio.ExportData(
            config=self._config,
            content="volumes",
            unit="m3" if self._units == "metric" else "ft3",
            vertical_domain="depth",
            domain_reference="msl",
            subfolder="volumes",
            classification=self.classification,
            name=self.grid_name.lower(),
            rep_include=False,
        )
        meta = edata.generate_metadata(self._dataframe)
        out = edata.export(self._dataframe)

        _logger.debug("Volume result to: %s", out)
        return ExportResult(
            items=[
                ExportResultItem(
                    absolute_path=Path(meta["file"]["absolute_path"]),
                    relative_path=Path(meta["file"]["relative_path"]),
                )
            ],
        )

    def export(self) -> ExportResult:
        """Export the volume table."""
        return self._export_volume_table()


@experimental
def export_volumetrics(
    project: Any,
    grid_name: str,
    volume_job_name: str,
    config_path: str | Path = "../../fmuconfig/output/global_variables.yml",
    classification: str | None = None,
) -> ExportResult:
    """Simplified interface when exporting volume tables (and assosiated data) from RMS.

        Args:
        project: The 'magic' project variable in RMS.
        grid_name: Name of 3D grid model in RMS.
        volume_job_name: Name of the volume job.
        config_path: Optional. Path to the global_variables file. As default, it assumes
            the current standard in FMU:
            ``'../../fmuconfig/output/global_variables.yml'``
        classification: Optional. Use 'internal' or 'restricted'. If not specified the
            default classification found in the global config will be used.

    Note:
        This function is experimental and may change in future versions.
    """

    return _ExportVolumetricsRMS(
        project,
        grid_name,
        volume_job_name,
        config_path=config_path,
        classification=classification,
    ).export()


# keep the old name for now but not log (will be removed soon as we expect close to
# zero usage so far)
def export_rms_volumetrics(*args, **kwargs) -> ExportResult:  # type: ignore
    """Deprecated function. Use export_volumetrics instead."""
    warnings.warn(
        "export_rms_volumetrics is deprecated and will be removed in a future release. "
        "Use export_volumetrics instead.",
        FutureWarning,
        stacklevel=2,
    )
    return export_volumetrics(*args, **kwargs)
