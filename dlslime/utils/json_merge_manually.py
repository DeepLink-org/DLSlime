import re
from json_merger import _merge_json
import argparse
import logging
from pathlib import Path
from typing import List
from datetime import datetime

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Recursively scan the input directory for JSON files and merge them into a single "
            "output JSON. The tool will search through "
            "all subdirectories to find JSON files and combine them."
            " Example usage: python json_merge_manually.py -i input_dir [-o merged_output.json]"
        )
    )
    
    parser.add_argument(
        "-i", "--input-dir",
        type=Path,
        required=True,
        help="Path to the input directory containing profiler result directories."
    )
    
    current_time = datetime.now()
    default_output = f"merged_trace_{current_time.strftime('%Y%m%d_%H%M%S')}.json"
    
    parser.add_argument(
        "-o", "--output-file",
        type=Path,
        default=Path(default_output),
        help=f"Path to the output merged JSON file. Default: {default_output}"
    )

    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable debug logging output."
    )

    args = parser.parse_args()

    input_dir: Path = args.input_dir
    output_file: Path = args.output_file
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format="%(asctime)s - %(levelname)s - %(message)s")

    # Check if input directory exists
    if not input_dir.exists():
        logging.error(f"Input directory '{input_dir}' does not exist.")
        return
    
    if not input_dir.is_dir():
        logging.error(f"Input path '{input_dir}' is not a directory.")
        return

    # Recursively search for all JSON files
    json_files_to_merge: List[Path] = list(input_dir.rglob('*.json'))
    
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

    logging.info(f"Successfully merged {len(json_files_to_merge)} JSON files into '{output_file}'.")


if __name__ == "__main__":
    main()