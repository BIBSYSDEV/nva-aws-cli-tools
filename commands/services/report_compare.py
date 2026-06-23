import json
import re
from typing import Any

import polars as pl
from rich.console import Console
from rich.markup import escape
from rich.table import Table

URL_PREFIX = re.compile(r"https?://[^/]+")
URL_PREFIX_PATTERN = r"^https?://[^/]+"

CHANGED = "changed"
ADDED = "added"
REMOVED = "removed"
CURRENT_SUFFIX = "__current"
BASELINE_COUNT = "baseline_count"
CURRENT_COUNT = "current_count"
DEFAULT_XLSX_KEY = (
    "NVAID",
    "PERSONLOPENR",
    "INSTITUSJON",
    "TITTEL",
    "ETTERNAVN",
    "FORNAVN",
)
FLOAT_TOLERANCE = 1e-9
FLOAT_ROUND_DECIMALS = 9
IDENTICAL_MESSAGE = "[green]No differences — reports are identical ✓[/green]"

console = Console()


def diff_json(baseline: Any, current: Any) -> list[dict[str, Any]]:
    diffs: list[dict[str, Any]] = []
    _walk(baseline, current, "", diffs)
    return diffs


def diff_dataframes(
    baseline: pl.DataFrame,
    current: pl.DataFrame,
    key_columns: list[str] | None = None,
) -> dict[str, Any]:
    baseline = _normalize_url_columns(baseline)
    current = _normalize_url_columns(current)
    common_columns = [
        column for column in baseline.columns if column in current.columns
    ]
    keys = [
        column
        for column in (key_columns or DEFAULT_XLSX_KEY)
        if column in common_columns
    ] or common_columns
    ambiguous = _ambiguous_keys(baseline, current, keys)
    baseline_unique = _exclude_keys(baseline, ambiguous, keys)
    current_unique = _exclude_keys(current, ambiguous, keys)
    ambiguous_removed, ambiguous_added = _ambiguous_diff(
        baseline, current, ambiguous, keys, common_columns
    )
    return {
        "key_columns": keys,
        "columns_only_in_baseline": [
            column for column in baseline.columns if column not in current.columns
        ],
        "columns_only_in_current": [
            column for column in current.columns if column not in baseline.columns
        ],
        "removed": _rows_missing_from(baseline_unique, current_unique, keys),
        "added": _rows_missing_from(current_unique, baseline_unique, keys),
        "changed": _changed_cells(
            baseline_unique, current_unique, keys, common_columns
        ),
        "ambiguous_removed": ambiguous_removed,
        "ambiguous_added": ambiguous_added,
        "numeric_totals": _numeric_totals_diff(baseline, current, common_columns),
    }


def render_json_diff(diffs: list[dict[str, Any]]) -> None:
    if not diffs:
        console.print(IDENTICAL_MESSAGE)
        return
    table = Table(title=f"{len(diffs)} difference(s) found")
    table.add_column("Change", style="bold")
    table.add_column("Path")
    table.add_column("Baseline", style="yellow")
    table.add_column("Current", style="cyan")
    for diff in diffs:
        table.add_row(
            diff["kind"],
            escape(diff["path"]),
            escape(_format_value(diff.get("baseline"))),
            escape(_format_value(diff.get("current"))),
        )
    console.print(table)


def render_dataframe_diff(result: dict[str, Any]) -> None:
    if _is_identical(result):
        console.print(IDENTICAL_MESSAGE)
        return
    if result["columns_only_in_baseline"]:
        console.print(
            f"[yellow]Columns only in baseline:[/yellow] {result['columns_only_in_baseline']}"
        )
    if result["columns_only_in_current"]:
        console.print(
            f"[cyan]Columns only in current:[/cyan] {result['columns_only_in_current']}"
        )
    _render_numeric_totals(result["numeric_totals"])
    _render_changed(result["changed"], result["key_columns"])
    _render_rows("Rows only in baseline (removed)", result["removed"])
    _render_rows("Rows only in current (added)", result["added"])
    _render_rows(
        "Ambiguous keys only in baseline (key not unique — raw rows)",
        result["ambiguous_removed"],
    )
    _render_rows(
        "Ambiguous keys only in current (key not unique — raw rows)",
        result["ambiguous_added"],
    )


def _is_identical(result: dict[str, Any]) -> bool:
    return (
        not result["columns_only_in_baseline"]
        and not result["columns_only_in_current"]
        and not result["changed"]
        and result["removed"].is_empty()
        and result["added"].is_empty()
        and result["ambiguous_removed"].is_empty()
        and result["ambiguous_added"].is_empty()
    )


def _render_changed(changes: list[dict[str, Any]], key_columns: list[str]) -> None:
    if not changes:
        return
    table = Table(title=f"{len(changes)} changed cell(s)")
    for key_column in key_columns:
        table.add_column(key_column, style="bold")
    table.add_column("Column", style="bold")
    table.add_column("Baseline", style="yellow", justify="right")
    table.add_column("Current", style="cyan", justify="right")
    for change in changes:
        key_values = [escape(_format_value(change["key"][key])) for key in key_columns]
        table.add_row(
            *key_values,
            change["column"],
            escape(_format_value(change["baseline"])),
            escape(_format_value(change["current"])),
        )
    console.print(table)


def _render_numeric_totals(totals: list[dict[str, Any]]) -> None:
    if not totals:
        return
    table = Table(title="Numeric column totals that changed")
    table.add_column("Column", style="bold")
    table.add_column("Baseline", style="yellow", justify="right")
    table.add_column("Current", style="cyan", justify="right")
    table.add_column("Delta", justify="right")
    for total in totals:
        table.add_row(
            total["column"],
            str(total["baseline"]),
            str(total["current"]),
            str(total["delta"]),
        )
    console.print(table)


def _render_rows(title: str, frame: pl.DataFrame) -> None:
    if frame.is_empty():
        return
    console.print(f"[bold]{title} ({frame.height}):[/bold]")
    console.print(frame)


def _format_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _walk(baseline: Any, current: Any, path: str, diffs: list[dict[str, Any]]) -> None:
    if isinstance(baseline, dict) and isinstance(current, dict):
        _walk_dicts(baseline, current, path, diffs)
    elif isinstance(baseline, list) and isinstance(current, list):
        _walk_lists(baseline, current, path, diffs)
    elif _normalize(baseline) != _normalize(current):
        diffs.append(
            {"path": path, "kind": CHANGED, "baseline": baseline, "current": current}
        )


def _walk_dicts(
    baseline: dict, current: dict, path: str, diffs: list[dict[str, Any]]
) -> None:
    for key in list(baseline) + [key for key in current if key not in baseline]:
        child_path = f"{path}.{key}" if path else str(key)
        if key not in current:
            diffs.append(
                {"path": child_path, "kind": REMOVED, "baseline": baseline[key]}
            )
        elif key not in baseline:
            diffs.append({"path": child_path, "kind": ADDED, "current": current[key]})
        else:
            _walk(baseline[key], current[key], child_path, diffs)


def _walk_lists(
    baseline: list, current: list, path: str, diffs: list[dict[str, Any]]
) -> None:
    baseline_by_key = _index_by_key(baseline)
    current_by_key = _index_by_key(current)
    ordered_keys = list(baseline_by_key) + [
        key for key in current_by_key if key not in baseline_by_key
    ]
    for key in ordered_keys:
        child_path = f"{path}[{key}]"
        if key not in current_by_key:
            diffs.append(
                {"path": child_path, "kind": REMOVED, "baseline": baseline_by_key[key]}
            )
        elif key not in baseline_by_key:
            diffs.append(
                {"path": child_path, "kind": ADDED, "current": current_by_key[key]}
            )
        else:
            _walk(baseline_by_key[key], current_by_key[key], child_path, diffs)


def _index_by_key(items: list) -> dict[Any, Any]:
    indexed: dict[Any, Any] = {}
    for position, item in enumerate(items):
        if isinstance(item, dict) and "id" in item:
            indexed[_normalize(item["id"])] = item
        else:
            indexed[position] = item
    return indexed


def _normalize(value: Any) -> Any:
    if isinstance(value, str):
        return URL_PREFIX.sub("", value)
    return value


def _ambiguous_keys(
    baseline: pl.DataFrame, current: pl.DataFrame, keys: list[str]
) -> pl.DataFrame:
    baseline_dups = baseline.group_by(keys).len().filter(pl.col("len") > 1).select(keys)
    current_dups = current.group_by(keys).len().filter(pl.col("len") > 1).select(keys)
    return pl.concat([baseline_dups, current_dups]).unique()


def _exclude_keys(
    frame: pl.DataFrame, ambiguous: pl.DataFrame, keys: list[str]
) -> pl.DataFrame:
    if ambiguous.is_empty():
        return frame
    return frame.join(ambiguous, on=keys, how="anti", nulls_equal=True)


def _rows_missing_from(
    left: pl.DataFrame, right: pl.DataFrame, keys: list[str]
) -> pl.DataFrame:
    return left.join(right.select(keys), on=keys, how="anti", nulls_equal=True)


def _changed_cells(
    baseline: pl.DataFrame,
    current: pl.DataFrame,
    keys: list[str],
    common_columns: list[str],
) -> list[dict[str, Any]]:
    value_columns = [column for column in common_columns if column not in keys]
    if not value_columns:
        return []
    joined = baseline.join(
        current.select(keys + value_columns),
        on=keys,
        how="inner",
        suffix=CURRENT_SUFFIX,
        nulls_equal=True,
    )
    changes: list[dict[str, Any]] = []
    for column in value_columns:
        current_column = f"{column}{CURRENT_SUFFIX}"
        if current_column not in joined.columns:
            continue
        differing = joined.filter(
            _differs_expr(column, current_column, baseline.schema[column])
        )
        for row in differing.select(keys + [column, current_column]).iter_rows(
            named=True
        ):
            changes.append(
                {
                    "key": {key: row[key] for key in keys},
                    "column": column,
                    "baseline": row[column],
                    "current": row[current_column],
                }
            )
    return changes


def _differs_expr(column: str, current_column: str, dtype: pl.DataType) -> pl.Expr:
    baseline_value = pl.col(column)
    current_value = pl.col(current_column)
    if dtype.is_float():
        both_present = baseline_value.is_not_null() & current_value.is_not_null()
        one_missing = baseline_value.is_null() != current_value.is_null()
        return one_missing | (
            both_present & ((baseline_value - current_value).abs() > FLOAT_TOLERANCE)
        )
    return baseline_value.ne_missing(current_value)


def _ambiguous_diff(
    baseline: pl.DataFrame,
    current: pl.DataFrame,
    ambiguous: pl.DataFrame,
    keys: list[str],
    common_columns: list[str],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    if ambiguous.is_empty():
        empty = baseline.head(0)
        return empty, empty
    baseline_rows = _round_float_columns(
        baseline.join(ambiguous, on=keys, how="semi", nulls_equal=True)
    )
    current_rows = _round_float_columns(
        current.join(ambiguous, on=keys, how="semi", nulls_equal=True)
    )
    return _multiset_diff(baseline_rows, current_rows, common_columns)


def _normalize_url_columns(frame: pl.DataFrame) -> pl.DataFrame:
    string_columns = [
        column for column, dtype in frame.schema.items() if dtype == pl.String
    ]
    if not string_columns:
        return frame
    return frame.with_columns(
        pl.col(column).str.replace(URL_PREFIX_PATTERN, "") for column in string_columns
    )


def _round_float_columns(frame: pl.DataFrame) -> pl.DataFrame:
    float_columns = [
        column for column, dtype in frame.schema.items() if dtype.is_float()
    ]
    if not float_columns:
        return frame
    return frame.with_columns(
        pl.col(column).round(FLOAT_ROUND_DECIMALS) for column in float_columns
    )


def _multiset_diff(
    baseline: pl.DataFrame, current: pl.DataFrame, common_columns: list[str]
) -> tuple[pl.DataFrame, pl.DataFrame]:
    baseline_counts = (
        baseline.select(common_columns)
        .group_by(common_columns)
        .agg(pl.len().alias(BASELINE_COUNT))
    )
    current_counts = (
        current.select(common_columns)
        .group_by(common_columns)
        .agg(pl.len().alias(CURRENT_COUNT))
    )
    merged = baseline_counts.join(
        current_counts, on=common_columns, how="full", nulls_equal=True, coalesce=True
    ).with_columns(
        pl.col(BASELINE_COUNT).fill_null(0), pl.col(CURRENT_COUNT).fill_null(0)
    )
    removed = merged.filter(pl.col(BASELINE_COUNT) > pl.col(CURRENT_COUNT))
    added = merged.filter(pl.col(CURRENT_COUNT) > pl.col(BASELINE_COUNT))
    return removed, added


def _numeric_totals_diff(
    baseline: pl.DataFrame, current: pl.DataFrame, common_columns: list[str]
) -> list[dict[str, Any]]:
    totals: list[dict[str, Any]] = []
    for column in common_columns:
        if not baseline[column].dtype.is_numeric():
            continue
        baseline_sum = baseline[column].sum()
        current_sum = current[column].sum()
        if abs(current_sum - baseline_sum) > FLOAT_TOLERANCE:
            totals.append(
                {
                    "column": column,
                    "baseline": baseline_sum,
                    "current": current_sum,
                    "delta": current_sum - baseline_sum,
                }
            )
    return totals
