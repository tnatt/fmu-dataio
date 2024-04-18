"""Module for DataIO _FileData

Populate and verify stuff in the 'file' block in fmu (partial excpetion is checksum_md5
as this is convinient to populate later, on demand)
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Final, Optional
from warnings import warn

from fmu.dataio._definitions import FmuContext
from fmu.dataio._logging import null_logger
from fmu.dataio._utils import (
    compute_md5_using_temp_file,
)
from fmu.dataio.datastructure.meta import meta

logger: Final = null_logger(__name__)

if TYPE_CHECKING:
    from fmu.dataio import ExportData, types

    from .objectdata._provider import ObjectDataProvider


@dataclass
class FileDataProvider:
    """Class for providing metadata for the 'files' block in fmu-dataio.

    Example::

        file:
            relative_path: ... (relative to case)
            absolute_path: ...
    """

    # input
    dataio: ExportData
    objdata: ObjectDataProvider
    rootpath: Path = field(default_factory=Path)
    itername: str = ""
    realname: str = ""
    obj: Optional[types.Inferrable] = field(default=None)
    compute_md5: bool = False

    # storing results in these variables
    forcefolder_is_absolute: bool = field(default=False, init=False)

    @property
    def name(self) -> str:
        return self.dataio.name or self.objdata.name

    def get_metadata(self) -> meta.File:
        relpath = self._get_path()
        relative_path, absolute_path = self._derive_filedata_generic(relpath)
        logger.info("Returning metadata pydantic model meta.File")
        return meta.File(
            absolute_path=absolute_path,
            relative_path=relative_path,
            checksum_md5=self._compute_md5() if self.compute_md5 else None,
        )

    def _derive_filedata_generic(self, inrelpath: Path) -> tuple[Path, Path]:
        """This works with both normal data and symlinks."""
        stem = self._get_filestem()

        path = Path(inrelpath) / stem.lower()
        path = path.with_suffix(path.suffix + self.objdata.extension)

        # resolve() will fix ".." e.g. change '/some/path/../other' to '/some/other'
        abspath = path.resolve()

        try:
            str(abspath).encode("ascii")
        except UnicodeEncodeError:
            print(f"!! Path has non-ascii elements which is not supported: {abspath}")
            raise

        if self.forcefolder_is_absolute:
            # may become meaningsless as forcefolder can be something else, but will try
            try:
                relpath = path.relative_to(self.rootpath)
            except ValueError as verr:
                if ("does not start with" in str(verr)) or (
                    "not in the subpath of" in str(verr)
                ):
                    relpath = abspath
                    logger.info(
                        "Relative path equal to absolute path due to forcefolder "
                        "with absolute path deviating for rootpath %s",
                        self.rootpath,
                    )
                else:
                    raise
        else:
            relpath = path.relative_to(self.rootpath)

        logger.info("Derived filedata")
        return relpath, abspath

    def _compute_md5(self) -> str:
        """Compute an MD5 sum using a temporary file."""
        if self.obj is None:
            raise ValueError("Can't compute MD5 sum without an object.")
        return compute_md5_using_temp_file(
            self.obj, self.objdata.extension, self.dataio._usefmtflag
        )

    def _get_filestem(self) -> str:
        """Construct the file"""

        if not self.name:
            raise ValueError("The 'name' entry is missing for constructing a file name")
        if not self.objdata.time0 and self.objdata.time1:
            raise ValueError("Not legal: 'time0' is missing while 'time1' is present")

        stem = self.name.lower()
        if self.dataio.tagname:
            stem += "--" + self.dataio.tagname.lower()
        if self.dataio.parent:
            stem = self.dataio.parent.lower() + "--" + stem

        if self.objdata.time0 and not self.objdata.time1:
            stem += "--" + (str(self.objdata.time0)[0:10]).replace("-", "")

        elif self.objdata.time0 and self.objdata.time1:
            monitor = (str(self.objdata.time1)[0:10]).replace("-", "")
            base = (str(self.objdata.time0)[0:10]).replace("-", "")
            if monitor == base:
                warn(
                    "The monitor date and base date are equal", UserWarning
                )  # TODO: consider add clocktimes in such cases?
            if self.dataio.filename_timedata_reverse:  # class variable
                stem += "--" + base + "_" + monitor
            else:
                stem += "--" + monitor + "_" + base

        # remove unwanted characters
        stem = stem.replace(".", "_").replace(" ", "_")

        # avoid multiple double underscores
        while "__" in stem:
            stem = stem.replace("__", "_")

        # treat norwegian special letters
        # BUG(?): What about germen letter like "Ü"?
        stem = stem.replace("æ", "ae")
        stem = stem.replace("ø", "oe")
        return stem.replace("å", "aa")

    def _get_path(self) -> Path:
        """Construct and get the folder path(s)."""
        mode = self.dataio.fmu_context
        outroot = deepcopy(self.rootpath)

        logger.info("FMU context is %s", mode)
        if mode == FmuContext.REALIZATION:
            if self.realname:
                outroot = outroot / self.realname  # TODO: if missing self.realname?

            if self.itername:
                outroot = outroot / self.itername

        outroot = outroot / "share"

        if mode == FmuContext.PREPROCESSED:
            outroot = outroot / "preprocessed"
            if self.dataio.forcefolder and self.dataio.forcefolder.startswith("/"):
                raise ValueError(
                    "Cannot use absolute path to 'forcefolder' with preprocessed data"
                )

        if mode != FmuContext.PREPROCESSED:
            if self.dataio.is_observation:
                outroot = outroot / "observations"
            else:
                outroot = outroot / "results"

        dest = outroot / self.objdata.efolder  # e.g. "maps"

        if self.dataio.forcefolder and self.dataio.forcefolder.startswith("/"):
            if not self.dataio.allow_forcefolder_absolute:
                raise ValueError(
                    "The forcefolder includes an absolute path, i.e. "
                    "starting with '/'. This is strongly discouraged and is only "
                    "allowed if classvariable allow_forcefolder_absolute is set to True"
                )
            warn("Using absolute paths in forcefolder is not recommended!")

            # absolute if starts with "/", otherwise relative to outroot
            dest = Path(self.dataio.forcefolder).absolute()
            self.forcefolder_is_absolute = True

        return dest if not self.dataio.subfolder else dest / self.dataio.subfolder
