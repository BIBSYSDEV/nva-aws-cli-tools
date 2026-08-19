#!/usr/bin/env bash
#
# Generate the NVA access reports (roles per user) as Excel files.
# Reads the environment from the AWS_PROFILE environment variable.
#
# Usage:
#   AWS_PROFILE=<profile> ./docs/generate-access-reports.sh [output-dir]
#
set -euo pipefail

if [[ -z "${AWS_PROFILE:-}" ]]; then
  echo "AWS_PROFILE is not set. Export it first, e.g. AWS_PROFILE=sikt-nva-prod" >&2
  exit 1
fi

OUTPUT_DIR="${1:-$PWD}"
mkdir -p "$OUTPUT_DIR"
OUTPUT_DIR="$(cd "$OUTPUT_DIR" && pwd)"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

DATE="$(date +%F)"
CURATOR_ROLES="Nvi-Curator,Support-Curator,Publishing-Curator,Doi-Curator,Curator-thesis,Curator-thesis-embargo"

echo "Profile: $AWS_PROFILE"
echo "Output : $OUTPUT_DIR"
echo

echo "== Role summary =="
uv run cli.py users role-summary

export_roles() {
  local description="$1"
  local filename="$2"
  shift 2
  echo
  echo "== $description -> $OUTPUT_DIR/$filename =="
  uv run cli.py users export-roles "$@" --output "$OUTPUT_DIR/$filename"
}

export_roles "App-admin"          "nva-app-admin-$DATE.xlsx"          --include-roles "App-admin"
export_roles "Editors"            "nva-redaktor-$DATE.xlsx"           --include-roles "Editor"
export_roles "Institution-admin"  "nva-admin-$DATE.xlsx"              --include-roles "Institution-admin"
export_roles "Internal-importer"  "nva-internal-importer-$DATE.xlsx"  --include-roles "Internal-importer"
export_roles "All curators"       "nva-curators-$DATE.xlsx"           --include-roles "$CURATOR_ROLES"
export_roles "All except only Creator" "nva-all-except-only-creator-$DATE.xlsx" --exclude-only-roles "Creator"

echo
echo "Done. Reports written to $OUTPUT_DIR"
