"""
Complete demo workflow for the DataHub Soda Plugin.

This script demonstrates:
1. Setting up the handler
2. Processing scan results
3. Validating governance policies
4. Error handling
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from datahub_soda_plugin.handler import DataHubSodaHandler

# Configuration
DATAHUB_SERVER_URL = "http://localhost:8080"
DATAHUB_TOKEN = None  # Set your token here if needed
ENVIRONMENT = "PROD"


def print_section(title: str):
    """Print a formatted section header."""
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def load_scan_result(file_path: str) -> dict:
    """Load scan result from JSON file."""
    path = Path(__file__).parent / file_path
    if not path.exists():
        print(f"❌ Error: Scan result file not found: {path}")
        sys.exit(1)
    
    with open(path, "r") as f:
        return json.load(f)


def main():
    """Main demo workflow."""
    print_section("DataHub Soda Plugin Demo")
    
    # Step 1: Initialize the handler
    print_section("Step 1: Initialize DataHub Soda Handler")
    print(f"Connecting to DataHub at: {DATAHUB_SERVER_URL}")
    
    handler = DataHubSodaHandler(
        server_url=DATAHUB_SERVER_URL,
        env=ENVIRONMENT,
        token=DATAHUB_TOKEN,
        platform_instance_map={
            "postgres": "prod_postgres_instance",
            "snowflake": "analytics_warehouse",
        },
        graceful_exceptions=True,
    )
    print("✅ Handler initialized successfully")
    
    # Step 2: Load scan result
    print_section("Step 2: Load Soda Scan Result")
    scan_result = load_scan_result("example_scan_result.json")
    
    print(f"📊 Scan ID: {scan_result.get('scanId', 'N/A')}")
    print(f"📊 Data Source: {scan_result.get('dataSourceName', 'N/A')}")
    print(f"📊 Tables: {len(scan_result.get('tables', []))}")
    print(f"📊 Checks: {len(scan_result.get('checks', []))}")
    
    # Display check details
    print("\nCheck Details:")
    for i, check in enumerate(scan_result.get('checks', []), 1):
        outcome_emoji = "✅" if check.get('outcome', '').lower() in ['pass', 'passed'] else "❌"
        print(f"  {i}. {outcome_emoji} {check.get('name', 'Unknown')} - {check.get('outcome', 'unknown')}")
    
    # Step 3: Process scan result
    print_section("Step 3: Process Scan Result and Send to DataHub")
    print("Sending assertions to DataHub...")
    
    try:
        result = handler.process_scan_result(
            scan_result=scan_result,
            scan_timestamp=datetime.now(timezone.utc),
        )
        
        if result["status"] == "success":
            print(f"✅ Successfully processed scan!")
            print(f"   📤 Assertions sent: {result.get('assertions_sent', 0)}")
            print(f"   🆔 Scan ID: {result.get('scan_id', 'N/A')}")
        else:
            print(f"❌ Error: {result.get('error', 'Unknown error')}")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Failed to process scan result: {e}")
        sys.exit(1)
    
    # Step 4: Validate governance policies
    print_section("Step 4: Validate Governance Policies")
    
    # Extract dataset URN from first table
    if scan_result.get('tables'):
        table = scan_result['tables'][0]
        data_source = scan_result.get('dataSourceName', 'postgres')
        db_name = table.get('databaseName', 'mydb')
        schema_name = table.get('schemaName', 'public')
        table_name = table.get('tableName', 'users')
        
        # Build dataset URN (simplified - in practice, use handler method)
        dataset_urn = f"urn:li:dataset:(urn:li:dataPlatform:{data_source},{db_name}.{schema_name}.{table_name},{ENVIRONMENT})"
        
        print(f"Validating policies for dataset: {dataset_urn}")
        
        policies = [
            {"name": "Data Quality Policy", "type": "data_quality"},
            {"name": "Schema Validation Policy", "type": "schema"},
            {"name": "Completeness Policy", "type": "completeness"},
        ]
        
        validation_result = handler.validate_governance_policies(
            dataset_urn=dataset_urn,
            policies=policies,
            scan_result=scan_result,
        )
        
        print(f"\n📋 Governance Policy Validation Results:")
        print(f"   Policies validated: {validation_result['policies_validated']}")
        print(f"   Compliant: {validation_result['compliant']}")
        print(f"   Non-compliant: {validation_result['non_compliant']}")
        
        print("\n📝 Policy Details:")
        for detail in validation_result.get('details', []):
            print(f"   • {detail.get('policy', 'Unknown')} ({detail.get('type', 'unknown')}): {detail.get('status', 'unknown')}")
    
    # Step 5: Summary
    print_section("Demo Complete!")
    print("✅ Scan results successfully sent to DataHub")
    print("✅ Governance policies validated")
    print("\n📖 Next Steps:")
    print("   1. Open DataHub UI to view the assertions")
    print("   2. Navigate to the dataset page")
    print("   3. Check the 'Assertions' tab")
    print("   4. Review data quality metrics")
    print(f"\n🌐 DataHub UI: {DATAHUB_SERVER_URL.replace(':8080', ':9002')}")


if __name__ == "__main__":
    main()
