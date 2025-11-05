#!/usr/bin/env python3
"""
Extract handle identifiers from JSONL files.

This script processes JSONL files containing resource records and extracts
handle identifiers from two sources:
1. Top-level 'handle' field (if present)
2. HandleIdentifier entries in 'additionalIdentifiers' array

Output format:
{
  "<handleValue>": {
    "nvaIds": [<identifier>, ...],
    "sourceName": [<sourceName>, ...]
  }
}


report count to migrate: jq '[.[] | select(.sourceName | index("nva@sikt") | not)] | length' additional_identifier_handles.json
report duplicated: jq 'to_entries | map(select(.value.nvaIds | length > 1))' top_level_handles.json
"""

import json
from pathlib import Path
from typing import Dict, Set
import sys


def extract_handle_value(handle_url: str) -> str:
    """Extract handle value from URL like https://hdl.handle.net/11250/5271720"""
    if handle_url.startswith('https://hdl.handle.net/'):
        return handle_url.replace('https://hdl.handle.net/', '')
    return handle_url


def process_jsonl_file(file_path: Path, top_handles: Dict, additional_handles: Dict) -> None:
    """Process a single JSONL file and accumulate handle data."""
    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            try:
                obj = json.loads(line)

                nva_id = obj.get('identifier')
                if not nva_id:
                    continue

                # Check for top-level handle field (if it exists)
                if 'handle' in obj:
                    handle_value = extract_handle_value(obj['handle'])
                    if handle_value not in top_handles:
                        top_handles[handle_value] = {
                            'nvaIds': [],
                            'sourceName': []
                        }
                    if nva_id not in top_handles[handle_value]['nvaIds']:
                        top_handles[handle_value]['nvaIds'].append(nva_id)

                # Check for HandleIdentifier in additionalIdentifiers
                additional_identifiers = obj.get('additionalIdentifiers', [])
                for identifier in additional_identifiers:
                    if identifier.get('type') == 'HandleIdentifier':
                        handle_value = extract_handle_value(identifier.get('value', ''))
                        source_name = identifier.get('sourceName', '')

                        if handle_value not in additional_handles:
                            additional_handles[handle_value] = {
                                'nvaIds': [],
                                'sourceName': []
                            }

                        if nva_id not in additional_handles[handle_value]['nvaIds']:
                            additional_handles[handle_value]['nvaIds'].append(nva_id)

                        if source_name and source_name not in additional_handles[handle_value]['sourceName']:
                            additional_handles[handle_value]['sourceName'].append(source_name)

            except json.JSONDecodeError as e:
                print(f"Error parsing {file_path} line {line_num}: {e}", file=sys.stderr)
            except Exception as e:
                print(f"Error processing {file_path} line {line_num}: {e}", file=sys.stderr)


def process_directory(directory_path: str) -> None:
    """Process all JSONL files in a directory."""
    dir_path = Path(directory_path)

    if not dir_path.exists():
        print(f"Error: Directory {directory_path} does not exist", file=sys.stderr)
        sys.exit(1)

    if not dir_path.is_dir():
        print(f"Error: {directory_path} is not a directory", file=sys.stderr)
        sys.exit(1)

    top_handles = {}
    additional_handles = {}

    jsonl_files = sorted(dir_path.glob('*.jsonl'))

    if not jsonl_files:
        print(f"Warning: No JSONL files found in {directory_path}", file=sys.stderr)
        return

    print(f"Processing {len(jsonl_files)} JSONL files...", file=sys.stderr)

    for idx, file_path in enumerate(jsonl_files, 1):
        if idx % 100 == 0:
            print(f"Processed {idx}/{len(jsonl_files)} files...", file=sys.stderr)
        process_jsonl_file(file_path, top_handles, additional_handles)

    print(f"Finished processing {len(jsonl_files)} files", file=sys.stderr)
    print(f"Found {len(top_handles)} top-level handles", file=sys.stderr)
    print(f"Found {len(additional_handles)} handles in additionalIdentifiers", file=sys.stderr)

    # Write output files
    output_dir = dir_path

    top_handles_file = output_dir / 'top_level_handles.json'
    with open(top_handles_file, 'w', encoding='utf-8') as f:
        json.dump(top_handles, f, indent=2, ensure_ascii=False)
    print(f"Wrote top-level handles to: {top_handles_file}", file=sys.stderr)

    additional_handles_file = output_dir / 'additional_identifier_handles.json'
    with open(additional_handles_file, 'w', encoding='utf-8') as f:
        json.dump(additional_handles, f, indent=2, ensure_ascii=False)
    print(f"Wrote additional identifier handles to: {additional_handles_file}", file=sys.stderr)

    # Print statistics about duplicates
    print("\n=== Statistics ===", file=sys.stderr)

    if top_handles:
        duplicates = sum(1 for data in top_handles.values() if len(data['nvaIds']) > 1)
        print(f"Top-level handles with multiple NVA IDs: {duplicates}/{len(top_handles)}", file=sys.stderr)

    if additional_handles:
        duplicates = sum(1 for data in additional_handles.values() if len(data['nvaIds']) > 1)
        print(f"Additional identifier handles with multiple NVA IDs: {duplicates}/{len(additional_handles)}", file=sys.stderr)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python3 extract_handles.py <directory_path>", file=sys.stderr)
        sys.exit(1)

    directory_path = sys.argv[1]
    process_directory(directory_path)
