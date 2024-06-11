from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

import numpy as np
import pandas as pd
import xtgeo

from fmu.dataio._definitions import ValidFormats
from fmu.dataio._logging import null_logger
from fmu.dataio._utils import npfloat_to_float
from fmu.dataio.datastructure.meta.content import BoundingBox2D, BoundingBox3D
from fmu.dataio.datastructure.meta.enums import FMUClassEnum
from fmu.dataio.datastructure.meta.specification import (
    CPGridPropertySpecification,
    CPGridSpecification,
    CubeSpecification,
    PointSpecification,
    PolygonsSpecification,
    SurfaceSpecification,
)

from ._base import (
    DerivedObjectDescriptor,
    ObjectDataProvider,
)

if TYPE_CHECKING:
    import pandas as pd

logger: Final = null_logger(__name__)


@dataclass
class RegularSurfaceDataProvider(ObjectDataProvider):
    obj: xtgeo.RegularSurface

    @property
    def classname(self) -> FMUClassEnum:
        return FMUClassEnum.surface

    @property
    def extension(self) -> str:
        return self._validate_get_ext(self.fmt, ValidFormats.surface)

    @property
    def fmt(self) -> str:
        return self.dataio.surface_fformat

    def get_bbox(self) -> BoundingBox2D | BoundingBox3D:
        """
        Derive data.bbox for xtgeo.RegularSurface. The zmin/zmax fields represents
        the minimum/maximum surface values and should be absent in the metadata if the
        surface only has undefined values.
        """
        logger.info("Get bbox for RegularSurface")

        if np.isfinite(self.obj.values).any():
            return BoundingBox3D(
                xmin=float(self.obj.xmin),
                xmax=float(self.obj.xmax),
                ymin=float(self.obj.ymin),
                ymax=float(self.obj.ymax),
                zmin=float(self.obj.values.min()),
                zmax=float(self.obj.values.max()),
            )

        return BoundingBox2D(
            xmin=float(self.obj.xmin),
            xmax=float(self.obj.xmax),
            ymin=float(self.obj.ymin),
            ymax=float(self.obj.ymax),
        )

    def get_spec(self) -> SurfaceSpecification:
        """Derive data.spec for xtgeo.RegularSurface."""
        logger.info("Get spec for RegularSurface")

        required = self.obj.metadata.required
        return SurfaceSpecification(
            ncol=npfloat_to_float(required["ncol"]),
            nrow=npfloat_to_float(required["nrow"]),
            xori=npfloat_to_float(required["xori"]),
            yori=npfloat_to_float(required["yori"]),
            xinc=npfloat_to_float(required["xinc"]),
            yinc=npfloat_to_float(required["yinc"]),
            yflip=npfloat_to_float(required["yflip"]),
            rotation=npfloat_to_float(required["rotation"]),
            undef=1.0e30,
        )

    def get_objectdata(self) -> DerivedObjectDescriptor:
        """Derive object data for xtgeo.RegularSurface."""
        return DerivedObjectDescriptor(
            layout="regular",
            efolder="maps",
            table_index=None,
        )


@dataclass
class PolygonsDataProvider(ObjectDataProvider):
    obj: xtgeo.Polygons

    @property
    def classname(self) -> FMUClassEnum:
        return FMUClassEnum.polygons

    @property
    def extension(self) -> str:
        return self._validate_get_ext(self.fmt, ValidFormats.polygons)

    @property
    def fmt(self) -> str:
        return self.dataio.polygons_fformat

    def get_bbox(self) -> BoundingBox3D:
        """Derive data.bbox for xtgeo.Polygons"""
        logger.info("Get bbox for Polygons")

        xmin, xmax, ymin, ymax, zmin, zmax = self.obj.get_boundary()
        return BoundingBox3D(
            xmin=float(xmin),
            xmax=float(xmax),
            ymin=float(ymin),
            ymax=float(ymax),
            zmin=float(zmin),
            zmax=float(zmax),
        )

    def get_spec(self) -> PolygonsSpecification:
        """Derive data.spec for xtgeo.Polygons."""
        logger.info("Get spec for Polygons")

        return PolygonsSpecification(
            npolys=np.unique(
                self.obj.get_dataframe(copy=False)[self.obj.pname].values
            ).size
        )

    def get_objectdata(self) -> DerivedObjectDescriptor:
        """Derive object data for xtgeo.Polygons."""
        return DerivedObjectDescriptor(
            layout="unset",
            efolder="polygons",
            table_index=None,
        )


@dataclass
class PointsDataProvider(ObjectDataProvider):
    obj: xtgeo.Points

    @property
    def classname(self) -> FMUClassEnum:
        return FMUClassEnum.points

    @property
    def extension(self) -> str:
        return self._validate_get_ext(self.fmt, ValidFormats.points)

    @property
    def fmt(self) -> str:
        return self.dataio.points_fformat

    @property
    def obj_dataframe(self) -> pd.DataFrame:
        """Returns a dataframe of the referenced xtgeo.Points object."""
        return self.obj.get_dataframe(copy=False)

    def get_bbox(self) -> BoundingBox3D:
        """Derive data.bbox for xtgeo.Points."""
        logger.info("Get bbox for Points")

        df = self.obj_dataframe
        return BoundingBox3D(
            xmin=float(df[self.obj.xname].min()),
            xmax=float(df[self.obj.xname].max()),
            ymax=float(df[self.obj.yname].min()),
            ymin=float(df[self.obj.yname].max()),
            zmin=float(df[self.obj.zname].min()),
            zmax=float(df[self.obj.zname].max()),
        )

    def get_spec(self) -> PointSpecification:
        """Derive data.spec for xtgeo.Points."""
        logger.info("Get spec for Points")

        df = self.obj_dataframe
        return PointSpecification(
            attributes=list(df.columns[3:]) if len(df.columns) > 3 else None,
            size=int(df.size),
        )

    def get_objectdata(self) -> DerivedObjectDescriptor:
        """Derive object data for xtgeo.Points."""
        return DerivedObjectDescriptor(
            layout="unset",
            efolder="points",
            table_index=None,
        )


@dataclass
class CubeDataProvider(ObjectDataProvider):
    obj: xtgeo.Cube

    @property
    def classname(self) -> FMUClassEnum:
        return FMUClassEnum.cube

    @property
    def extension(self) -> str:
        return self._validate_get_ext(self.fmt, ValidFormats.cube)

    @property
    def fmt(self) -> str:
        return self.dataio.cube_fformat

    def get_bbox(self) -> BoundingBox3D:
        """Derive data.bbox for xtgeo.Cube."""
        logger.info("Get bbox for Cube")

        # current xtgeo is missing xmin, xmax etc attributes for cube, so need
        # to compute (simplify when xtgeo has this):
        xmin, ymin = 1.0e23, 1.0e23
        xmax, ymax = -xmin, -ymin

        for corner in (
            (1, 1),
            (1, self.obj.nrow),
            (self.obj.ncol, 1),
            (self.obj.ncol, self.obj.nrow),
        ):
            xco, yco = self.obj.get_xy_value_from_ij(*corner)
            xmin = min(xmin, xco)
            xmax = max(xmax, xco)
            ymin = min(ymin, yco)
            ymax = max(ymax, yco)

        return BoundingBox3D(
            xmin=float(xmin),
            xmax=float(xmax),
            ymin=float(ymin),
            ymax=float(ymax),
            zmin=float(self.obj.zori),
            zmax=float(self.obj.zori + self.obj.zinc * (self.obj.nlay - 1)),
        )

    def get_spec(self) -> CubeSpecification:
        """Derive data.spec for xtgeo.Cube."""
        logger.info("Get spec for Cube")

        required = self.obj.metadata.required
        return CubeSpecification(
            ncol=npfloat_to_float(required["ncol"]),
            nrow=npfloat_to_float(required["nrow"]),
            nlay=npfloat_to_float(required["nlay"]),
            xori=npfloat_to_float(required["xori"]),
            yori=npfloat_to_float(required["yori"]),
            zori=npfloat_to_float(required["zori"]),
            xinc=npfloat_to_float(required["xinc"]),
            yinc=npfloat_to_float(required["yinc"]),
            zinc=npfloat_to_float(required["zinc"]),
            yflip=npfloat_to_float(required["yflip"]),
            zflip=npfloat_to_float(required["zflip"]),
            rotation=npfloat_to_float(required["rotation"]),
            undef=npfloat_to_float(required["undef"]),
        )

    def get_objectdata(self) -> DerivedObjectDescriptor:
        """Derive object data for xtgeo.Cube."""
        return DerivedObjectDescriptor(
            layout="regular",
            efolder="cubes",
            table_index=None,
        )


@dataclass
class CPGridDataProvider(ObjectDataProvider):
    obj: xtgeo.Grid

    @property
    def classname(self) -> FMUClassEnum:
        return FMUClassEnum.cpgrid

    @property
    def extension(self) -> str:
        return self._validate_get_ext(self.fmt, ValidFormats.grid)

    @property
    def fmt(self) -> str:
        return self.dataio.grid_fformat

    def get_bbox(self) -> BoundingBox3D:
        """Derive data.bbox for xtgeo.Grid."""
        logger.info("Get bbox for Grid geometry")

        geox = self.obj.get_geometrics(
            cellcenter=False,
            allcells=True,
            return_dict=True,
        )
        return BoundingBox3D(
            xmin=round(float(geox["xmin"]), 4),
            xmax=round(float(geox["xmax"]), 4),
            ymin=round(float(geox["ymin"]), 4),
            ymax=round(float(geox["ymax"]), 4),
            zmin=round(float(geox["zmin"]), 4),
            zmax=round(float(geox["zmax"]), 4),
        )

    def get_spec(self) -> CPGridSpecification:
        """Derive data.spec for xtgeo.Grid."""
        logger.info("Get spec for Grid geometry")

        required = self.obj.metadata.required
        return CPGridSpecification(
            ncol=npfloat_to_float(required["ncol"]),
            nrow=npfloat_to_float(required["nrow"]),
            nlay=npfloat_to_float(required["nlay"]),
            xshift=npfloat_to_float(required["xshift"]),
            yshift=npfloat_to_float(required["yshift"]),
            zshift=npfloat_to_float(required["zshift"]),
            xscale=npfloat_to_float(required["xscale"]),
            yscale=npfloat_to_float(required["yscale"]),
            zscale=npfloat_to_float(required["zscale"]),
        )

    def get_objectdata(self) -> DerivedObjectDescriptor:
        """Derive object data for xtgeo.Grid."""
        return DerivedObjectDescriptor(
            layout="cornerpoint",
            efolder="grids",
            table_index=None,
        )


@dataclass
class CPGridPropertyDataProvider(ObjectDataProvider):
    obj: xtgeo.GridProperty

    @property
    def classname(self) -> FMUClassEnum:
        return FMUClassEnum.cpgrid_property

    @property
    def extension(self) -> str:
        return self._validate_get_ext(self.fmt, ValidFormats.grid)

    @property
    def fmt(self) -> str:
        return self.dataio.grid_fformat

    def get_bbox(self) -> None:
        """Derive data.bbox for xtgeo.GridProperty."""

    def get_spec(self) -> CPGridPropertySpecification:
        """Derive data.spec for xtgeo.GridProperty."""
        logger.info("Get spec for GridProperty")

        return CPGridPropertySpecification(
            nrow=self.obj.nrow,
            ncol=self.obj.ncol,
            nlay=self.obj.nlay,
        )

    def get_objectdata(self) -> DerivedObjectDescriptor:
        """Derive object data for xtgeo.GridProperty."""
        return DerivedObjectDescriptor(
            layout="cornerpoint",
            efolder="grids",
            table_index=None,
        )
