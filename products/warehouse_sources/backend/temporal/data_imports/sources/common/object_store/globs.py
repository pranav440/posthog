import re

from posthog.dataclasses import frozen

from products.warehouse_sources.backend.temporal.data_imports.sources.common.object_store.errors import (
    INVALID_CONFIG_ERROR,
    ObjectStorageError,
)

GLOB_CHARACTERS = frozenset("*?{")
# Characters ClickHouse does not document as glob syntax, but that other glob dialects use. A path
# that holds them could mean either thing, so it is refused instead of read one way.
_AMBIGUOUS_CHARACTERS = frozenset("[]}")
_NUMERIC_RANGE = re.compile(r"(\d+)\.\.(\d+)")


@frozen
class _NumericRange:
    group: str
    low: int
    high: int
    # Zero-padded ranges such as `{01..10}` only match numbers written with this many digits.
    width: int | None


@frozen
class ClickHouseGlob:
    """A path glob with the ClickHouse `s3()` semantics, matched against a whole path.

    `*` matches any characters except `/`, `**` matches any characters including `/`, `?` matches
    one character except `/`, `{a,b}` matches one of the literal alternatives, and `{N..M}` matches
    a whole number from N to M.
    """

    pattern: str
    regex: re.Pattern[str]
    ranges: tuple[_NumericRange, ...] = ()

    def matches(self, path: str) -> bool:
        match = self.regex.fullmatch(path)
        if match is None:
            return False
        for numeric in self.ranges:
            digits = match.group(numeric.group)
            if numeric.width is not None:
                if len(digits) != numeric.width:
                    return False
            elif len(digits) > 1 and digits.startswith("0"):
                return False
            if not numeric.low <= int(digits) <= numeric.high:
                return False
        return True


def has_glob(path: str) -> bool:
    return any(character in GLOB_CHARACTERS for character in path)


def glob_listing_prefix(pattern: str) -> str:
    """The folder that holds every path the glob can match: the segments before the first glob."""
    literal: list[str] = []
    segments = pattern.split("/")
    for index, segment in enumerate(segments):
        if has_glob(segment):
            return "/".join(literal)
        if index < len(segments) - 1:
            literal.append(segment)
    # No glob at all: the pattern names one file, and a listing of that path finds it.
    return pattern


def _invalid(pattern: str, detail: str) -> ObjectStorageError:
    return ObjectStorageError(f"{INVALID_CONFIG_ERROR}: the file pattern '{pattern}' {detail}.")


def compile_clickhouse_glob(pattern: str) -> ClickHouseGlob:
    parts: list[str] = []
    ranges: list[_NumericRange] = []
    index = 0
    while index < len(pattern):
        character = pattern[index]
        if pattern.startswith("**", index):
            parts.append(".*")
            index += 2
            continue
        if character == "*":
            parts.append("[^/]*")
        elif character == "?":
            parts.append("[^/]")
        elif character == "{":
            end = pattern.find("}", index)
            if end == -1:
                raise _invalid(pattern, "has a '{' with no closing '}'")
            body = pattern[index + 1 : end]
            if not body or any(nested in GLOB_CHARACTERS or nested in _AMBIGUOUS_CHARACTERS for nested in body):
                raise _invalid(pattern, "has a '{...}' group PostHog can't read")
            numeric = _NUMERIC_RANGE.fullmatch(body)
            if numeric is not None:
                start, stop = numeric.group(1), numeric.group(2)
                low, high = sorted((int(start), int(stop)))
                padded = (len(start) > 1 and start.startswith("0")) or (len(stop) > 1 and stop.startswith("0"))
                group = f"range{len(ranges)}"
                ranges.append(
                    _NumericRange(group=group, low=low, high=high, width=max(len(start), len(stop)) if padded else None)
                )
                parts.append(f"(?P<{group}>\\d+)")
            else:
                parts.append("(?:" + "|".join(re.escape(option) for option in body.split(",")) + ")")
            index = end + 1
            continue
        elif character in _AMBIGUOUS_CHARACTERS:
            raise _invalid(pattern, f"has a '{character}' that PostHog can't read as a file pattern")
        else:
            parts.append(re.escape(character))
        index += 1
    return ClickHouseGlob(pattern=pattern, regex=re.compile("".join(parts), re.DOTALL), ranges=tuple(ranges))
