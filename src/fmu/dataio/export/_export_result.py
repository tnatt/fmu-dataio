from pathlib import Path
from typing import List

from pydantic import BaseModel


class ExportResultItem(BaseModel):
    absolute_path: Path
    relative_path: Path


class ExportResult(BaseModel):
    items: List[ExportResultItem]
