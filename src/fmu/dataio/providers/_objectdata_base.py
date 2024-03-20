from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Final, Literal, NamedTuple, Optional, TypeVar
from warnings import warn

from fmu.dataio import dataio, types
from fmu.dataio._definitions import ConfigurationError
from fmu.dataio._logging import null_logger
from fmu.dataio._utils import generate_description, parse_timedata
from fmu.dataio.datastructure._internal.internal import AllowedContent

logger: Final = null_logger(__name__)

V = TypeVar("V")


@dataclass
class DerivedObjectDescriptor:
    subtype: Literal[
        "RegularSurface",
        "Polygons",
        "Points",
        "RegularCube",
        "CPGrid",
        "CPGridProperty",
        "DataFrame",
        "JSON",
        "ArrowTable",
    ]
    classname: Literal[
        "surface",
        "polygons",
        "points",
        "cube",
        "cpgrid",
        "cpgrid_property",
        "table",
        "dictionary",
    ]
    layout: Literal[
        "regular",
        "unset",
        "cornerpoint",
        "table",
        "dictionary",
    ]
    efolder: (
        Literal[
            "maps",
            "polygons",
            "points",
            "cubes",
            "grids",
            "tables",
            "dictionaries",
        ]
        | str
    )
    fmt: str
    extension: str
    spec: Dict[str, Any]
    bbox: Dict[str, Any]
    table_index: Optional[list[str]]


class SpecificationAndBoundingBox(NamedTuple):
    spec: Dict[str, Any]
    bbox: Dict[str, Any]


@dataclass
class TimedataValueLabel:
    value: str
    label: str

    @staticmethod
    def from_list(arr: list) -> TimedataValueLabel:
        return TimedataValueLabel(
            value=datetime.strptime(str(arr[0]), "%Y%m%d").isoformat(),
            label=arr[1] if len(arr) == 2 else "",
        )


@dataclass
class TimedataLegacyFormat:
    time: list[TimedataValueLabel]


@dataclass
class TimedataFormat:
    t0: Optional[TimedataValueLabel]
    t1: Optional[TimedataValueLabel]


@dataclass
class DerivedNamedStratigraphy:
    name: str
    alias: list[str]

    stratigraphic: bool
    stratigraphic_alias: list[str]

    offset: int | None
    base: str | None
    top: str | None


def derive_name(
    export: dataio.ExportData,
    obj: types.Inferrable,
) -> str:
    """
    Derives and returns a name for an export operation based on the
    provided ExportData instance and a 'sniffable' object.
    """
    if name := export.name:
        return name

    if isinstance(name := getattr(obj, "name", ""), str):
        return name

    return ""


@dataclass
class ObjectDataProvider(ABC):
    """Base class for providing metadata for data objects in fmu-dataio, e.g. a surface.

    The metadata for the 'data' are constructed by:

    * Investigating (parsing) the object (e.g. a XTGeo RegularSurface) itself
    * Combine the object info with user settings, globalconfig and class variables
    * OR
    * investigate current metadata if that is provided
    """

    # input fields
    obj: types.Inferrable
    dataio: dataio.ExportData
    meta_existing: dict = field(default_factory=dict)

    # result properties; the most important is metadata which IS the 'data' part in
    # the resulting metadata. But other variables needed later are also given
    # as instance properties in addition (for simplicity in other classes/functions)
    bbox: dict = field(default_factory=dict)
    classname: str = field(default="")
    efolder: str = field(default="")
    extension: str = field(default="")
    fmt: str = field(default="")
    layout: str = field(default="")
    metadata: dict = field(default_factory=dict)
    name: str = field(default="")
    specs: dict = field(default_factory=dict)
    subtype: str = field(default="")
    time0: str = field(default="")
    time1: str = field(default="")

    @staticmethod
    def _validate_get_ext(fmt: str, subtype: str, validator: dict[str, V]) -> V:
        """Validate that fmt (file format) matches data and return legal extension."""
        try:
            return validator[fmt]
        except KeyError:
            raise ConfigurationError(
                f"The file format {fmt} is not supported. ",
                f"Valid {subtype} formats are: {list(validator.keys())}",
            )

    def _derive_name_stratigraphy(self) -> DerivedNamedStratigraphy:
        """Derive the name and stratigraphy for the object; may have several sources.

        If not in input settings it is tried to be inferred from the xtgeo/pandas/...
        object. The name is then checked towards the stratigraphy list, and name is
        replaced with official stratigraphic name if found in static metadata
        `stratigraphy`. For example, if "TopValysar" is the model name and the actual
        name is "Valysar Top Fm." that latter name will be used.
        """
        name = derive_name(self.dataio, self.obj)

        # next check if usename has a "truename" and/or aliases from the config
        strat = self.dataio.config.get("stratigraphy", {})
        no_start_or_missing_name = strat is None or name not in strat

        rv = DerivedNamedStratigraphy(
            name=name if no_start_or_missing_name else strat[name].get("name", name),
            alias=[] if no_start_or_missing_name else strat[name].get("alias", []),
            stratigraphic=False
            if no_start_or_missing_name
            else strat[name].get("stratigraphic", False),
            stratigraphic_alias=[]
            if no_start_or_missing_name
            else strat[name].get("stratigraphic_alias"),
            offset=None if no_start_or_missing_name else strat[name].get("offset"),
            top=None if no_start_or_missing_name else strat[name].get("top"),
            base=None if no_start_or_missing_name else strat[name].get("base"),
        )

        if not no_start_or_missing_name and rv.name != "name":
            rv.alias.append(name)

        return rv

    def _process_content(self) -> tuple[str | dict, dict | None]:
        """Work with the `content` metadata"""

        # content == "unset" is not wanted, but in case metadata has been produced while
        # doing a preprocessing step first, and this step is re-using metadata, the
        # check is not done.
        if self.dataio._usecontent == "unset" and (
            self.dataio.reuse_metadata_rule is None
            or self.dataio.reuse_metadata_rule != "preprocessed"
        ):
            allowed_fields = ", ".join(AllowedContent.model_fields.keys())
            warn(
                "The <content> is not provided which defaults to 'unset'. "
                "It is strongly recommended that content is given explicitly! "
                f"\n\nValid contents are: {allowed_fields} "
                "\n\nThis list can be extended upon request and need.",
                UserWarning,
            )

        content = self.dataio._usecontent
        content_spesific = None

        # Outgoing content is always a string, but it can be given as a dict if content-
        # specific information is to be included in the metadata.
        # In that case, it shall be inserted in the data block as a key with name as the
        # content, e.g. "seismic" or "field_outline"
        if self.dataio._content_specific is not None:
            content_spesific = self.dataio._content_specific

        return content, content_spesific

    def _derive_timedata(
        self,
    ) -> Optional[TimedataFormat | TimedataLegacyFormat]:
        """Format input timedata to metadata

        New format:
            When using two dates, input convention is
                -[[newestdate, "monitor"], [oldestdate,"base"]]
            but it is possible to turn around. But in the metadata the output t0
            shall always be older than t1 so need to check, and by general rule the file
            will be some--time1_time0 where time1 is the newest (unless a class
            variable is set for those who wants it turned around).
        """

        tdata = self.dataio.timedata
        use_legacy_format: bool = self.dataio.legacy_time_format

        if not tdata:
            return None

        if len(tdata) == 1:
            start = TimedataValueLabel.from_list(tdata[0])
            self.time0 = start.value
            return (
                TimedataLegacyFormat([start])
                if use_legacy_format
                else TimedataFormat(start, None)
            )

        if len(tdata) == 2:
            start, stop = (
                TimedataValueLabel.from_list(tdata[0]),
                TimedataValueLabel.from_list(tdata[1]),
            )

            if datetime.fromisoformat(start.value) > datetime.fromisoformat(stop.value):
                start, stop = stop, start

            self.time0, self.time1 = start.value, stop.value

            return (
                TimedataLegacyFormat([start, stop])
                if use_legacy_format
                else TimedataFormat(start, stop)
            )

        return (
            TimedataLegacyFormat([])
            if use_legacy_format
            else TimedataFormat(None, None)
        )

    def _derive_from_existing(self) -> None:
        """Derive from existing metadata."""

        # do not change any items in 'data' block, as it may ruin e.g. stratigrapical
        # setting (i.e. changing data.name is not allowed)
        self.metadata = self.meta_existing["data"]
        self.name = self.meta_existing["data"]["name"]

        # derive the additional attributes needed later e.g. in Filedata provider:
        relpath = Path(self.meta_existing["file"]["relative_path"])
        if self.dataio.subfolder:
            self.efolder = relpath.parent.parent.name
        else:
            self.efolder = relpath.parent.name

        self.classname = self.meta_existing["class"]
        self.extension = relpath.suffix
        self.fmt = self.meta_existing["data"]["format"]

        # TODO: Clean up types below.
        self.time0, self.time1 = parse_timedata(self.meta_existing["data"])  # type: ignore

    def derive_metadata(self) -> None:
        """Main function here, will populate the metadata block for 'data'."""
        logger.info("Derive all metadata for data object...")

        if self.meta_existing:
            self._derive_from_existing()
            return

        namedstratigraphy = self._derive_name_stratigraphy()
        objres = self._derive_objectdata()
        if self.dataio.forcefolder and not self.dataio.forcefolder.startswith("/"):
            msg = (
                f"The standard folder name is overrided from {objres.efolder} to "
                f"{self.dataio.forcefolder}"
            )
            objres.efolder = self.dataio.forcefolder
            logger.info(msg)
            warn(msg, UserWarning)

        meta = self.metadata  # shortform

        meta["name"] = namedstratigraphy.name
        meta["stratigraphic"] = namedstratigraphy.stratigraphic
        meta["offset"] = namedstratigraphy.offset
        meta["alias"] = namedstratigraphy.alias
        meta["top"] = namedstratigraphy.top
        meta["base"] = namedstratigraphy.base

        content, content_spesific = self._process_content()
        meta["content"] = content
        if content_spesific:
            meta[self.dataio._usecontent] = content_spesific

        meta["tagname"] = self.dataio.tagname
        meta["format"] = objres.fmt
        meta["layout"] = objres.layout
        meta["unit"] = self.dataio.unit
        meta["vertical_domain"] = list(self.dataio.vertical_domain.keys())[0]
        meta["depth_reference"] = list(self.dataio.vertical_domain.values())[0]
        meta["spec"] = objres.spec
        meta["bbox"] = objres.bbox
        meta["table_index"] = objres.table_index
        meta["undef_is_zero"] = self.dataio.undef_is_zero

        # timedata:
        dt = self._derive_timedata()
        if isinstance(dt, TimedataLegacyFormat) and dt.time:
            meta["time"] = [asdict(v) for v in dt.time]
        elif isinstance(dt, TimedataFormat):
            if dt.t0 or dt.t1:
                meta["time"] = {}
            if t0 := dt.t0:
                meta["time"]["t0"] = asdict(t0)
            if t1 := dt.t1:
                meta["time"]["t1"] = asdict(t1)

        meta["is_prediction"] = self.dataio.is_prediction
        meta["is_observation"] = self.dataio.is_observation
        meta["description"] = generate_description(self.dataio.description)

        # the next is to give addition state variables identical values, and for
        # consistency these are derived after all eventual validation and directly from
        # the self.metadata fields:

        self.name = meta["name"]

        # then there are a few settings that are not in the ``data`` metadata, but
        # needed as data/variables in other classes:

        self.efolder = objres.efolder
        self.classname = objres.classname
        self.extension = objres.extension
        self.fmt = objres.fmt
        logger.info("Derive all metadata for data object... DONE")

    @abstractmethod
    def _derive_spec_and_bbox(self) -> SpecificationAndBoundingBox:
        raise NotImplementedError

    @abstractmethod
    def _derive_objectdata(self) -> DerivedObjectDescriptor:
        raise NotImplementedError
