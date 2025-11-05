#!/usr/bin/env python3
"""
Compare handles between top_level_handles.json and additional_identifier_handles.json.

This script finds handles that exist in top_level but not in additional_identifier.
"""

import json
import sys
from pathlib import Path


def compare_handles(top_level_file: str, additional_file: str, output_file: str = None) -> None:
    """Compare handles between two JSON files."""

    # Load both files
    with open(top_level_file, 'r', encoding='utf-8') as f:
        top_level = json.load(f)

    with open(additional_file, 'r', encoding='utf-8') as f:
        additional = json.load(f)

    # Find handles in top_level but not in additional
    top_level_only = {}
    for handle, data in top_level.items():
        if handle not in additional:
            top_level_only[handle] = data

    # Print statistics
    print(f"Top-level handles: {len(top_level)}", file=sys.stderr)
    print(f"Additional identifier handles: {len(additional)}", file=sys.stderr)
    print(f"Handles in top-level but NOT in additional: {len(top_level_only)}", file=sys.stderr)

    # Create report
    report = {
        "summary": {
            "top_level_count": len(top_level),
            "additional_count": len(additional),
            "top_level_only_count": len(top_level_only)
        },
        "handles_in_top_level_only": top_level_only
    }

    # Output report
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"Report written to: {output_file}", file=sys.stderr)
    else:
        print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: python3 compare_handles.py <top_level_handles.json> <additional_identifier_handles.json> [output_file.json]", file=sys.stderr)
        sys.exit(1)

    top_level_file = sys.argv[1]
    additional_file = sys.argv[2]
    output_file = sys.argv[3] if len(sys.argv) > 3 else None

    compare_handles(top_level_file, additional_file, output_file)
