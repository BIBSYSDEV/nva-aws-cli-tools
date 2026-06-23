# NVI report compare

Tooling for [NP-51365](https://sikt.atlassian.net/browse/NP-51365): capture 2025 baseline
numbers and verify they stay unchanged after re-evaluation.

The points for a closed period must never change. The workflow is: freeze a baseline
snapshot today, and after changes re-run the same reports and diff against the baseline —
expected diff for a closed period is none.

## Commands

One command per endpoint. All have the same shape: fetch fresh from the API, optionally
compare against a baseline file (`--baseline`), and optionally save the fetched data
(`--save`). When `--save` is omitted and you are not comparing, the fetched data is saved to
a default filename so a plain fetch is never lost.

### `nvi-all-periods` — all-periods summary JSON

Endpoint: `scientific-index/reports`.

| Option | Meaning |
| --- | --- |
| `--baseline FILE` | Compare the freshly fetched report against this baseline JSON file. |
| `--save FILE` | Save the freshly fetched JSON. Default name: `nvi_reports_<profile>_all_periods_<timestamp>.json`. |

```bash
# Freeze a baseline (replaces the old curl + token dance)
uv run cli.py reports nvi-all-periods --save all_periods_baseline.json

# Later: fetch fresh and verify nothing changed
uv run cli.py reports nvi-all-periods --baseline all_periods_baseline.json
```

### `nvi-institutions` — per-institution JSON

Endpoint: `scientific-index/reports/{year}/institutions`.

| Option | Meaning |
| --- | --- |
| `--year YEAR` | Reporting year (defaults to current year). |
| `--baseline FILE` | Compare the freshly fetched report against this baseline JSON file. |
| `--save FILE` | Save the freshly fetched JSON. Default name: `nvi_reports_<profile>_<year>_<timestamp>.json`. |

```bash
# Freeze a baseline
uv run cli.py reports nvi-institutions --year 2025 --save institutions_2025_baseline.json

# Later: fetch fresh and verify nothing changed
uv run cli.py reports nvi-institutions --year 2025 --baseline institutions_2025_baseline.json

# Fetch, save the new snapshot AND diff against the baseline
uv run cli.py reports nvi-institutions --year 2025 --baseline institutions_2025_baseline.json --save institutions_2025_now.json
```

### `author-shares` — Excel report

| Option | Meaning |
| --- | --- |
| `--year YEAR` | Reporting year (defaults to current year). |
| `--institution ID` | Institution identifier (e.g. `20754.0.0.0`). Defaults to all institutions. |
| `--baseline FILE` | Compare the freshly fetched xlsx against this baseline xlsx file. |
| `--save FILE` | Save the freshly fetched xlsx. When omitted and not comparing, a default name `author_shares_<profile>_<year>[_<institution>]_<timestamp>.xlsx` is used. |

```bash
# Freeze a baseline
uv run cli.py reports author-shares --year 2025 --save author_shares_2025_baseline.xlsx

# Later: fetch fresh and verify
uv run cli.py reports author-shares --year 2025 --baseline author_shares_2025_baseline.xlsx
```

## How the diff works

- **JSON** (`nvi-all-periods`, `nvi-institutions`): a structural deep-diff. List elements (periods, institutions,
  units) are matched by their `id`, not by position, so an added/removed/changed institution
  is reported with a precise path, e.g.
  `institutions[.../institutions/185.90.0.0].institutionSummary.totals.validPoints 6651.6703 → 9999.0`.
- **Excel** (`author-shares`): rows present in only one of the files (anti-join on common
  columns) plus a summary of numeric column totals that changed.
- Both **normalize away the URL host**, so a baseline taken from one environment can be
  compared against another (test vs prod) without the differing domains producing false
  positives — only the real numbers are compared.
- When everything matches: `No differences — reports are identical ✓`.

## Endpoints

| Report | Endpoint | Format |
| --- | --- | --- |
| All periods summary | `scientific-index/reports` | JSON |
| Per-institution for a year | `scientific-index/reports/{year}/institutions` | JSON |
| Author shares (xlsx) | `scientific-index/reports/{year}/institutions` with spreadsheet `Accept` | Excel |

All endpoints require a bearer token, which the CLI obtains automatically via `ApiClient`
using the current `AWS_PROFILE`.
