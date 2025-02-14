from __future__ import annotations

import warnings
from textwrap import dedent
from typing import Final, Type, Union

from typing_extensions import TypeAlias

from fmu.dataio._logging import null_logger
from fmu.dataio._models.fmu_results import data, enums
from fmu.dataio.providers.objectdata._export_models import (
    AllowedContentProperty,
    AllowedContentSeismic,
)

from ._base import Provider

logger: Final = null_logger(__name__)


def property_warn() -> None:
    warnings.warn(
        dedent(
            """
            When using content "property", please use a dictionary form, as
            more information is required. Example:
                content={"property": {"is_discrete": False}}

            The use of "property" will be disallowed in future versions."
            """
        ),
        FutureWarning,
    )


ContentDetailsModel: TypeAlias = Union[
    data.FieldOutline,
    data.FieldRegion,
    data.FluidContact,
    AllowedContentProperty,
    AllowedContentSeismic,
]


def content_details_factory(content: enums.Content) -> Type[ContentDetailsModel]:
    """Return the correct content_details model based on provided content."""
    if content == enums.Content.field_outline:
        return data.FieldOutline
    if content == enums.Content.field_region:
        return data.FieldRegion
    if content == enums.Content.fluid_contact:
        return data.FluidContact
    if content == enums.Content.property:
        return AllowedContentProperty
    if content == enums.Content.seismic:
        return AllowedContentSeismic
    raise ValueError(f"No content_details model exist for content {content.value}")


class ContentDetailsProvider(Provider):
    def __init__(
        self,
        content: enums.Content,
        content_details: dict[str, dict] | None,
    ):
        self.content = content
        self.content_details = content_details

        self._validate_input()

    def get_metadata(self) -> ContentDetailsModel:
        """
        Check the input and return a validated model for the given content details.
        Returns None if the content does not require any extra information.
        """

        if not self.content_details:
            raise ValueError("Missing content_details")

        return content_details_factory(self.content).model_validate(
            self.content_details
        )

    def _content_require_details(self) -> bool:
        """Flag if given content requires extra details"""
        try:
            content_details_factory(self.content)
            return True
        except ValueError:
            return False

    def _validate_input(self) -> None:
        """
        Various input validations. If content is input as a string, an
        error is raised if content_details is needed.
        """
        if self.content_details:
            if not self._content_require_details():
                warnings.warn(
                    f"The content {self.content} does not require extra information, "
                    "and should be input as a string."
                )
            else:
                if not isinstance(self.content_details, dict):
                    raise ValueError(
                        "Content is incorrectly formatted. When giving content as a "
                        "dict, it must be formatted as: "
                        "{'mycontent': {extra_key: extra_value}} where mycontent is "
                        "a string and in the list of valid contents, and extra keys in "
                        "associated dictionary must be valid keys for this content."
                    )

        if not self.content_details and self._content_require_details():
            # 'property' should be included below after a deprecation period
            if self.content == enums.Content.property:
                property_warn()
            else:
                raise ValueError(f"Content {self.content} requires additional input")
