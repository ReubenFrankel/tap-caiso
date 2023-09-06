"""Stream type classes for tap-caiso."""

from __future__ import annotations

import typing as t
from pathlib import Path

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_caiso.client import caisoStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class DemandStream(caisoStream):
    """Define custom stream."""

    @property
    def url_base(self) -> str:
        return super().url_base + "/demand.csv"
    
    name = "demand"
    path = url_base
    primary_keys: t.ClassVar[list[str]] = ["id"]

    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"  # noqa: ERA001
    schema = th.PropertiesList(
        th.Property("Time", th.TimeType),
        th.Property("Day ahead forecast", th.StringType),
        th.Property("Hour ahead forecast", th.StringType),
        th.Property("Current demand", th.StringType),
        th.Property("Demand response", th.StringType),
        th.Property("current_date", th.DateTimeType),
    ).to_dict()
