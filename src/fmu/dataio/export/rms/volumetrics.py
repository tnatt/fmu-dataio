from __future__ import annotations

import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

import pandas as pd

import fmu.dataio as dio
from fmu.dataio._logging import null_logger

from .._decorators import experimental
from .._export_result import ExportResult, ExportResultItem
from ._conditional_rms_imports import import_rms_package
from ._utils import _check_rmsapi_version, _get_rms_project_units, _load_config

rmsapi, rmsjobs = import_rms_package()

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
    config_path: str | Path = "../../fmuconfig/output/global_variables.yml"
    classification: str | None = None

    def __post_init__(self) -> None:
        _logger.debug("Process data, estiblish state prior to export.")
        self._config = _load_config(Path(self.config_path))
        self._volume_job = self._rms_volume_job_settings()
        self._volume_table_name = self._read_volume_table_name_from_job()
        self._dataframe = self._voltable_as_dataframe()
        _logger.debug("Process data... DONE")

    def _rms_volume_job_settings(self) -> dict:
        """Get information out from the RMS job API."""
        _logger.debug("RMS VOLJOB settings...")
        return rmsjobs.Job.get_job(
            owner=["Grid models", self.grid_name, "Grid"],
            type="Volumetrics",
            name=self.volume_job_name,
        ).get_arguments()

    def _read_volume_table_name_from_job(self) -> str:
        """Read the volume table name from RMS."""
        _logger.debug("Read volume table name from RMS...")
        voltable = self._volume_job.get("Report")
        if isinstance(voltable, list):
            voltable = voltable[0]

        volume_table_name = voltable.get("ReportTableName")
        if not volume_table_name:
            raise RuntimeError(
                "You need to configure output to Report file: Report table "
                "in the volumetric job. Provide a table name and rerun the job."
            )

        _logger.debug("The volume table name is %s", volume_table_name)
        return volume_table_name

    def _voltable_as_dataframe(self) -> pd.DataFrame:
        """Convert table to pandas dataframe"""
        _logger.debug("Read values and convert to pandas dataframe...")

        dict_values = (
            self.project.volumetric_tables[self._volume_table_name]
            .get_data_table()
            .to_dict()
        )
        _logger.debug("Dict values are: %s", dict_values)
        return (
            pd.DataFrame.from_dict(dict_values)
            .rename(columns=_RENAME_COLUMNS_FROM_RMS)
            .drop("REAL", axis=1, errors="ignore")
        )

    def _export_volume_table(self) -> ExportResult:
        """Do the actual volume table export using dataio setup."""

        edata = dio.ExportData(
            config=self._config,
            content="volumes",
            unit="m3" if _get_rms_project_units(self.project) == "metric" else "ft3",
            vertical_domain="depth",
            domain_reference="msl",
            subfolder="volumes",
            classification=self.classification,
            name=self.grid_name.lower(),
            rep_include=False,
        )
        out = edata.export(self._dataframe)
        _logger.debug("Volume result to: %s", out)

        meta = edata.generate_metadata(self._dataframe)
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
    _check_rmsapi_version(minimum_version="1.7")

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
