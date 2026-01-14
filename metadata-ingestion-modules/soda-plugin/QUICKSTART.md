# Quick Start Guide: DataHub Soda Plugin

This guide will help you set up and run the DataHub Soda plugin locally for testing and demonstration.

## Prerequisites

1. **Python 3.9+** installed
2. **DataHub instance** running (local or remote)
   - Quick start: `docker run -p 8080:8080 -p 8081:8081 acryldata/datahub-upgrade:headless -n`
   - Or use an existing DataHub deployment
3. **Soda Core** installed (optional, for generating scan results)

## Step 1: Install the Plugin

### Option A: Install from Source (Development)

```bash
cd metadata-ingestion-modules/soda-plugin

# Create virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install the plugin and dependencies
pip install -e .

# Or with development dependencies
pip install -e .[dev]
```

### Option B: Install via Gradle (DataHub Build System)

```bash
# From the datahub root directory
./gradlew :metadata-ingestion-modules:soda-plugin:install
```

## Step 2: Set Up DataHub Connection

Ensure your DataHub instance is running and accessible:

```bash
# Test DataHub connection
curl http://localhost:8080/health

# Or check in browser
open http://localhost:8080
```

If you need authentication, get your token from DataHub UI:
- Go to Settings → Access Tokens
- Create a new token

## Step 3: Prepare a Soda Scan Result

### Option A: Use the Example Scan Result

The plugin includes a sample scan result:

```bash
cd metadata-ingestion-modules/soda-plugin/examples
cat example_scan_result.json
```

### Option B: Generate a Real Scan Result

If you have Soda Core installed:

```bash
# Install Soda Core
pip install soda-core-postgresql  # or soda-core-snowflake, etc.

# Create a Soda configuration file (soda_config.yml)
# data_source postgres:
#   type: postgres
#   connection:
#     host: localhost
#     username: ${POSTGRES_USERNAME}
#     password: ${POSTGRES_PASSWORD}
#     database: mydb

# Run a scan and save results
soda scan -d postgres -c soda_config.yml -o scan_result.json
```

### Option C: Create a Custom Scan Result

Create a JSON file with this structure:

```json
{
  "scanId": "my_scan_001",
  "dataSourceName": "postgres",
  "tables": [
    {
      "tableName": "users",
      "schemaName": "public",
      "databaseName": "mydb"
    }
  ],
  "checks": [
    {
      "name": "users_id_not_null",
      "type": "missing_count",
      "definition": "SELECT COUNT(*) FROM users WHERE id IS NULL",
      "outcome": "pass",
      "table": "users",
      "schema": "public",
      "column": "id",
      "metrics": [
        {"id": "row_count", "value": 1000},
        {"id": "missing_count", "value": 0}
      ]
    }
  ]
}
```

## Step 4: Run the Plugin

### Using the CLI

```bash
# Basic usage
datahub-soda \
  --server-url http://localhost:8080 \
  --scan-result examples/example_scan_result.json \
  --env PROD

# With authentication token
datahub-soda \
  --server-url http://localhost:8080 \
  --token your-datahub-token \
  --scan-result examples/example_scan_result.json \
  --env PROD

# With platform instance mapping
datahub-soda \
  --server-url http://localhost:8080 \
  --scan-result examples/example_scan_result.json \
  --platform-instance-map platform_map.json \
  --env PROD
```

### Using Python API

Create a test script:

```python
# test_soda_integration.py
import json
from datahub_soda_plugin.handler import DataHubSodaHandler

# Initialize handler
handler = DataHubSodaHandler(
    server_url="http://localhost:8080",
    env="PROD",
    token="your-token-here",  # Optional
    platform_instance_map={
        "postgres": "prod_postgres",
    },
)

# Load scan result
with open("examples/example_scan_result.json", "r") as f:
    scan_result = json.load(f)

# Process and send to DataHub
result = handler.process_scan_result(scan_result)

print(f"Status: {result['status']}")
print(f"Assertions sent: {result.get('assertions_sent', 0)}")
```

Run it:
```bash
python test_soda_integration.py
```

## Step 5: Verify in DataHub UI

1. Open DataHub UI: http://localhost:9002 (or your DataHub URL)
2. Search for the dataset (e.g., "mydb.public.users")
3. Navigate to the dataset page
4. Check the "Assertions" tab to see the Soda checks
5. View assertion results and metrics

## Step 6: Test Governance Policy Validation

```python
from datahub_soda_plugin.handler import DataHubSodaHandler

handler = DataHubSodaHandler(server_url="http://localhost:8080")

# Example policies (in real usage, fetch from DataHub)
policies = [
    {"name": "Data Quality Policy", "type": "data_quality"},
    {"name": "Schema Validation", "type": "schema"},
]

dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.users,PROD)"

validation_result = handler.validate_governance_policies(
    dataset_urn=dataset_urn,
    policies=policies,
)

print(f"Policies validated: {validation_result['policies_validated']}")
```

## Troubleshooting

### Connection Issues

```bash
# Check if DataHub is running
curl http://localhost:8080/health

# Check network connectivity
ping localhost
```

### Authentication Errors

- Verify your token is correct
- Check token permissions in DataHub UI
- Ensure token hasn't expired

### Dataset Not Found

- Verify the dataset exists in DataHub
- Check the URN format matches your DataHub instance
- Ensure the platform name matches (postgres, snowflake, etc.)

### Import Errors

```bash
# Reinstall the plugin
pip install -e . --force-reinstall

# Check dependencies
pip list | grep -E "(datahub|soda)"
```

## Example Workflow for Blog Demo

1. **Start DataHub** (if not running)
   ```bash
   docker run -p 8080:8080 -p 8081:8081 acryldata/datahub-upgrade:headless -n
   ```

2. **Install the plugin**
   ```bash
   cd metadata-ingestion-modules/soda-plugin
   pip install -e .
   ```

3. **Run a Soda scan** (or use example)
   ```bash
   # Use example or real scan
   datahub-soda --server-url http://localhost:8080 --scan-result examples/example_scan_result.json
   ```

4. **View results in DataHub**
   - Open http://localhost:9002
   - Search for datasets
   - View assertions

5. **Show governance validation**
   - Run Python script with policy validation
   - Show compliance results

## Next Steps

- Integrate with CI/CD pipelines
- Set up automated scans
- Create custom check mappings
- Extend governance policy validation
