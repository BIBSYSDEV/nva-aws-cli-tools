import json
import re
from typing import Any

import polars as pl
from rich.console import Console
from rich.markup import escape
from rich.table import Table

URL_PREFIX = re.compile(r"https?://[^/]+")

CHANGED = "changed"
ADDED = "added"
REMOVED = "removed"
IDENTICAL_MESSAGE = "[green]No differences — reports are identical ✓[/green]"

console = Console()


def diff_json(baseline: Any, current: Any) -> list[dict[str, Any]]:
    diffs: list[dict[str, Any]] = []
    _walk(baseline, current, "", diffs)
    return diffs


def diff_dataframes(baseline: pl.DataFrame, current: pl.DataFrame) -> dict[str, Any]:
    common_columns = [
        column for column in baseline.columns if column in current.columns
    ]
    return {
        "columns_only_in_baseline": [
            column for column in baseline.columns if column not in current.columns
        ],
        "columns_only_in_current": [
            column for column in current.columns if column not in baseline.columns
        ],
        "rows_only_in_baseline": _anti_join(baseline, current, common_columns),
        "rows_only_in_current": _anti_join(current, baseline, common_columns),
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
    _render_rows(
        "Rows only in baseline (changed or removed)", result["rows_only_in_baseline"]
    )
    _render_rows(
        "Rows only in current (changed or added)", result["rows_only_in_current"]
    )


def _is_identical(result: dict[str, Any]) -> bool:
    return (
        not result["columns_only_in_baseline"]
        and not result["columns_only_in_current"]
        and result["rows_only_in_baseline"].is_empty()
        and result["rows_only_in_current"].is_empty()
    )


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


def _anti_join(
    left: pl.DataFrame, right: pl.DataFrame, common_columns: list[str]
) -> pl.DataFrame:
    if not common_columns:
        return left
    return left.join(right, on=common_columns, how="anti")


def _numeric_totals_diff(
    baseline: pl.DataFrame, current: pl.DataFrame, common_columns: list[str]
) -> list[dict[str, Any]]:
    totals: list[dict[str, Any]] = []
    for column in common_columns:
        if not baseline[column].dtype.is_numeric():
            continue
        baseline_sum = baseline[column].sum()
        current_sum = current[column].sum()
        if baseline_sum != current_sum:
            totals.append(
                {
                    "column": column,
                    "baseline": baseline_sum,
                    "current": current_sum,
                    "delta": current_sum - baseline_sum,
                }
            )
    return totals
