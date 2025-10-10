import re
from json_merger import _merge_json
import argparse
import logging
from pathlib import Path
from typing import List

# Paste your log text here
log_text = """
"""

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Scan the pasted `log_text` for profiler result directories, collect the JSON files "
            "contained in those directories, and merge them into a single output JSON using the "
            "project's _merge_json utilities. Provide the output path via -o/--output-file."
            " Example usage: python json_merge_manually.py -o merged_output.json"
        )
    )
    
    parser.add_argument(
        "-o", "--output-file",
        type=Path,
        required=True,
        help="Path to the output merged JSON file."
    )

    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable debug logging output."
    )

    args = parser.parse_args()

    output_file: Path = args.output_file
    # Configure basic logging so messages appear by default.
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format="%(asctime)s - %(levelname)s - %(message)s")
    
    # Configure the normal expression pattern to match the desired directories
    path_pattern = r'/docker_mnt/profiler_res/.+?_rank\d+_bs\d+_\d+'

    paths = sorted(re.findall(path_pattern, log_text))
    logging.info(f"find {len(paths)} directories matching the pattern.")

    json_files_to_merge: List[Path] = []
    for dir_str in paths:
        dir_path = Path(dir_str)
        
        if not dir_path.is_dir():
            print(f"Warning: Path '{dir_path}' is not a valid directory, skipping.")
            continue

        found_jsons = list(dir_path.glob('*.json'))
        
        if len(found_jsons) == 1:
            json_files_to_merge.append(found_jsons[0])
        elif len(found_jsons) == 0:
            print(f"Warning: No JSON files found in directory '{dir_path}'.")
        else:
            print(f"Warning: Found multiple JSON files ({len(found_jsons)}) in directory '{dir_path}', skipping.")


    if not json_files_to_merge:
        logging.warning("No JSON files found to merge.")
        return
        
    logging.info(f"Found {len(json_files_to_merge)} JSON files to merge.")

    _merge_json(
        to_merge_files=json_files_to_merge,
        output_json=output_file,
        compress=False,
        version=2
    )


if __name__ == "__main__":
    main()