"""
CLI tool for processing Soda scan results and sending them to DataHub.
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Optional

from datahub_soda_plugin.handler import DataHubSodaHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def load_scan_result(file_path: str) -> dict:
    """Load Soda scan result from JSON file."""
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load scan result from {file_path}: {e}")
        sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Process Soda scan results and send to DataHub"
    )
    parser.add_argument(
        "--server-url",
        required=True,
        help="DataHub GMS server URL (e.g., http://localhost:8080)",
    )
    parser.add_argument(
        "--token",
        help="DataHub authentication token",
    )
    parser.add_argument(
        "--scan-result",
        required=True,
        help="Path to Soda scan result JSON file",
    )
    parser.add_argument(
        "--env",
        default="PROD",
        help="DataHub environment (default: PROD)",
    )
    parser.add_argument(
        "--platform-alias",
        help="Platform alias to use instead of detected platform",
    )
    parser.add_argument(
        "--platform-instance-map",
        help="JSON file mapping datasource names to platform instances",
    )
    parser.add_argument(
        "--convert-urns-to-lowercase",
        action="store_true",
        help="Convert dataset names to lowercase in URNs",
    )
    parser.add_argument(
        "--timeout-sec",
        type=float,
        help="Request timeout in seconds",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Load platform instance map if provided
    platform_instance_map: Optional[dict] = None
    if args.platform_instance_map:
        try:
            with open(args.platform_instance_map, "r") as f:
                platform_instance_map = json.load(f)
        except Exception as e:
            logger.error(f"Failed to load platform instance map: {e}")
            sys.exit(1)

    # Load scan result
    scan_result = load_scan_result(args.scan_result)

    # Initialize handler
    handler = DataHubSodaHandler(
        server_url=args.server_url,
        env=args.env,
        platform_alias=args.platform_alias,
        platform_instance_map=platform_instance_map,
        token=args.token,
        timeout_sec=args.timeout_sec,
        convert_urns_to_lowercase=args.convert_urns_to_lowercase,
    )

    # Process scan result
    logger.info(f"Processing scan result from {args.scan_result}")
    result = handler.process_scan_result(scan_result)

    # Print results
    if result.get("status") == "success":
        logger.info(
            f"Successfully processed scan. Sent {result.get('assertions_sent', 0)} assertions."
        )
        sys.exit(0)
    else:
        logger.error(f"Failed to process scan: {result.get('error', 'Unknown error')}")
        sys.exit(1)


if __name__ == "__main__":
    main()
