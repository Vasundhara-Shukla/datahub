"""
Example usage of the DataHub Soda Plugin.

This example shows how to use the DataHubSodaHandler to process
Soda scan results and send them to DataHub.
"""

import json
from datetime import datetime, timezone

from datahub_soda_plugin.handler import DataHubSodaHandler


def main():
    """Example: Process a Soda scan result and send to DataHub."""
    # Initialize the handler
    handler = DataHubSodaHandler(
        server_url="http://localhost:8080",  # DataHub GMS server URL
        env="PROD",  # Environment name
        token="your-datahub-token",  # Optional: DataHub authentication token
        platform_instance_map={
            "postgres": "prod_postgres_instance",  # Map datasource to platform instance
        },
        graceful_exceptions=True,  # Suppress exceptions and return error dict
    )

    # Load scan result (in practice, this would come from Soda)
    with open("example_scan_result.json", "r") as f:
        scan_result = json.load(f)

    # Process the scan result
    result = handler.process_scan_result(
        scan_result=scan_result,
        scan_timestamp=datetime.now(timezone.utc),
    )

    # Check results
    if result["status"] == "success":
        print(f"✅ Successfully sent {result['assertions_sent']} assertions to DataHub")
        print(f"Scan ID: {result['scan_id']}")
    else:
        print(f"❌ Error: {result.get('error', 'Unknown error')}")

    # Example: Validate governance policies
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.users,PROD)"
    policies = [
        {"name": "Data Quality Policy", "type": "data_quality"},
        {"name": "Schema Validation Policy", "type": "schema"},
    ]

    validation_result = handler.validate_governance_policies(
        dataset_urn=dataset_urn,
        policies=policies,
        scan_result=scan_result,
    )

    print(f"\nGovernance Policy Validation:")
    print(f"  Policies validated: {validation_result['policies_validated']}")
    print(f"  Compliant: {validation_result['compliant']}")
    print(f"  Non-compliant: {validation_result['non_compliant']}")


if __name__ == "__main__":
    main()
