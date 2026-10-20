import re
from collections.abc import Callable, Mapping
from typing import Any, Literal, cast, get_args

import pyarrow as pa
import pyarrow.compute as pc

from posthog.dataclasses import frozen

# The types a CSV column can be cast to. Anything else stays a string column.
CsvColumnKind = Literal["integer", "float", "boolean", "date", "datetime"]
CSV_COLUMN_KINDS: frozenset[str] = frozenset(get_args(CsvColumnKind))

_WRAPPER = re.compile(r"^(?:Nullable|LowCardinality)\((?P<inner>.*)\)$")
# UInt64 and the 128/256-bit integers do not fit an Int64 column, so they stay strings.
_CLICKHOUSE_KINDS: dict[str, CsvColumnKind] = {
    "Int8": "integer",
    "Int16": "integer",
    "Int32": "integer",
    "Int64": "integer",
    "UInt8": "integer",
    "UInt16": "integer",
    "UInt32": "integer",
    "Float32": "float",
    "Float64": "float",
    "Bool": "boolean",
    "Date": "date",
    "Date32": "date",
    "DateTime": "datetime",
    "DateTime64": "datetime",
}
_ARROW_TYPES: dict[CsvColumnKind, pa.DataType] = {
    "integer": pa.int64(),
    "float": pa.float64(),
    "boolean": pa.bool_(),
    "date": pa.date32(),
    "datetime": pa.timestamp("us", tz="UTC"),
}
# The spellings ClickHouse accepts for a Bool in text formats.
_TRUE_VALUES = ["true", "1", "yes", "y", "on", "t", "enable"]
_FALSE_VALUES = ["false", "0", "no", "n", "off", "f", "disable"]
_NAIVE_TIMESTAMP = pa.timestamp("us")


@frozen
class CsvColumnType:
    name: str
    kind: CsvColumnKind


@frozen
class CsvCastResult:
    table: pa.Table
    # Per column, how many non-empty values could not be read as the column's type and became null.
    nulled_values: dict[str, int]


def csv_column_kind(clickhouse_type: str) -> CsvColumnKind | None:
    """The cast for a stored ClickHouse type, or None when the column stays a string."""
    inner = clickhouse_type.strip()
    while (wrapped := _WRAPPER.match(inner)) is not None:
        inner = wrapped.group("inner").strip()
    return _CLICKHOUSE_KINDS.get(inner.partition("(")[0])


def csv_column_types_from_columns(columns: Mapping[str, Any] | None) -> tuple[CsvColumnType, ...]:
    """The casts for a warehouse table's stored columns, in their stored order.

    A column ClickHouse marked as invalid never read as its type, so it stays a string.
    """
    column_types: list[CsvColumnType] = []
    for name, definition in (columns or {}).items():
        if isinstance(definition, str):
            clickhouse_type = definition
        elif isinstance(definition, Mapping):
            if definition.get("valid") is False:
                continue
            clickhouse_type = str(definition.get("clickhouse") or "")
        else:
            continue
        kind = csv_column_kind(clickhouse_type)
        if kind is not None:
            column_types.append(CsvColumnType(name=name, kind=kind))
    return tuple(column_types)


def csv_column_types_from_metadata(raw: object) -> tuple[CsvColumnType, ...]:
    if not isinstance(raw, Mapping):
        return ()
    return tuple(
        CsvColumnType(name=name, kind=cast(CsvColumnKind, kind))
        for name, kind in raw.items()
        if isinstance(name, str) and kind in CSV_COLUMN_KINDS
    )


def _cast_each(values: pa.Array, parse: Callable[[pa.Array], pa.Array], target: pa.DataType) -> pa.Array:
    parsed: list[Any] = []
    for value in values.to_pylist():
        if value is None:
            parsed.append(None)
            continue
        try:
            parsed.append(parse(pa.array([value], pa.string()))[0].as_py())
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError, OverflowError):
            parsed.append(None)
    return pa.array(parsed, target)


def _parse_number(target: pa.DataType) -> Callable[[pa.Array], pa.Array]:
    # Arrow refuses an explicit plus sign, which ClickHouse reads as part of the number.
    return lambda values: pc.cast(pc.replace_substring_regex(values, pattern=r"^\+", replacement=""), target)


def _parse_boolean(values: pa.Array) -> pa.Array:
    lowered = pc.utf8_lower(values)
    is_true = pc.is_in(lowered, value_set=pa.array(_TRUE_VALUES))
    is_false = pc.is_in(lowered, value_set=pa.array(_FALSE_VALUES))
    if not pc.all(pc.or_(pc.or_(is_true, is_false), pc.is_null(values))).as_py():
        raise pa.ArrowInvalid("not a boolean")
    return pc.if_else(pc.is_null(values), pa.scalar(None, pa.bool_()), is_true)


def _parse_datetime(values: pa.Array) -> pa.Array:
    # ClickHouse reads a DateTime without an offset as UTC, so a naive value is cast as one.
    try:
        return pc.cast(pc.cast(values, _NAIVE_TIMESTAMP), _ARROW_TYPES["datetime"])
    except pa.ArrowInvalid:
        return pc.cast(values, _ARROW_TYPES["datetime"])


def _parser(kind: CsvColumnKind) -> Callable[[pa.Array], pa.Array]:
    if kind == "boolean":
        return _parse_boolean
    if kind == "datetime":
        return _parse_datetime
    return _parse_number(_ARROW_TYPES[kind])


def cast_csv_columns(table: pa.Table, column_types: tuple[CsvColumnType, ...]) -> CsvCastResult:
    """Cast the string columns of a CSV batch to the stored types.

    A value that can't be read as its column's type becomes null rather than failing the file,
    and the caller reports how many values that happened to. The batch is cast as a whole first,
    so only a batch that holds such a value pays for the per-value pass.
    """
    nulled: dict[str, int] = {}
    for column_type in column_types:
        if column_type.name not in table.column_names:
            continue
        index = table.schema.get_field_index(column_type.name)
        values = pc.utf8_trim_whitespace(table.column(index).combine_chunks())
        # A quoted empty field is an empty string, which no typed column can hold.
        values = pc.if_else(pc.equal(values, ""), pa.scalar(None, pa.string()), values)
        target = _ARROW_TYPES[column_type.kind]
        parse = _parser(column_type.kind)
        try:
            cast_values = parse(values)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
            cast_values = _cast_each(values, parse, target)
            failed = cast_values.null_count - values.null_count
            if failed:
                nulled[column_type.name] = failed
        table = table.set_column(index, pa.field(column_type.name, target), cast_values)
    return CsvCastResult(table=table, nulled_values=nulled)
