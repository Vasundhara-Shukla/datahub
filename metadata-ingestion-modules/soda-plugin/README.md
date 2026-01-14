# DataHub Soda Plugin

A DataHub integration plugin for [Soda Core](https://docs.soda.io/soda-core/), an open-source data quality and testing framework. This plugin enables you to:

- **Ingest data quality checks** from Soda scans as DataHub assertions
- **Track data quality metrics** and validation results in DataHub
- **Validate governance policies** by mapping DataHub policies to Soda checks
- **Monitor data quality** across your data platform with centralized visibility

## Features

- ✅ Convert Soda scan results to DataHub assertions
- ✅ Ingest metadata about datasets, schemas, and quality metrics
- ✅ Map Soda check types to DataHub assertion operators
- ✅ Support for column-level and dataset-level checks
- ✅ Governance policy validation framework
- ✅ CLI tool for processing scan results
- ✅ Python API for programmatic usage

## Installation

### From Source

```bash
cd metadata-ingestion-modules/soda-plugin
pip install -e .
```

### With Development Dependencies

```bash
pip install -e .[dev]
```

### With Integration Test Dependencies

```bash
pip install -e .[dev,integration-tests]
```

## Quick Start

### Using the CLI

1. Run a Soda scan and save the results to a JSON file:

```bash
soda scan -d postgres -c configuration.yml -o scan_result.json
```

2. Process the scan result and send to DataHub:

```bash
datahub-soda \
  --server-url http://localhost:8080 \
  --token your-datahub-token \
  --scan-result scan_result.json \
  --env PROD
```

### Using the Python API

```python
from datahub_soda_plugin.handler import DataHubSodaHandler
import json

# Initialize handler
handler = DataHubSodaHandler(
    server_url="http://localhost:8080",
    env="PROD",
    token="your-datahub-token",
)

# Load scan result
with open("scan_result.json", "r") as f:
    scan_result = json.load(f)

# Process and send to DataHub
result = handler.process_scan_result(scan_result)

if result["status"] == "success":
    print(f"Sent {result['assertions_sent']} assertions to DataHub")
```

## Configuration

### Handler Parameters

- `server_url` (required): DataHub GMS server URL
- `env` (default: "PROD"): DataHub environment name
- `token` (optional): DataHub authentication token
- `platform_alias` (optional): Override detected platform name
- `platform_instance_map` (optional): Map datasource names to platform instances
- `convert_urns_to_lowercase` (default: False): Convert dataset names to lowercase
- `timeout_sec` (optional): Request timeout in seconds
- `graceful_exceptions` (default: True): Suppress exceptions and return error dict

### Platform Instance Mapping

You can map Soda datasource names to DataHub platform instances:

```python
handler = DataHubSodaHandler(
    server_url="http://localhost:8080",
    platform_instance_map={
        "postgres": "prod_postgres_instance",
        "snowflake": "analytics_warehouse",
    },
)
```

## Soda Scan Result Format

The plugin expects Soda scan results in the following JSON format:

```json
{
  "scanId": "scan_123",
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

## Check Type Mapping

The plugin maps common Soda check types to DataHub assertion operators:

| Soda Check Type | DataHub Operator | Scope |
|----------------|------------------|-------|
| `missing_count` / `null` | `NOT_NULL` | Column or Dataset |
| `duplicate_count` / `unique` | Native | Column |
| `row_count` / `count` | Native | Dataset |
| `schema` | Native | Schema |
| Other | Native | Column or Dataset |

## Governance Policy Validation

The plugin includes a framework for validating DataHub governance policies against Soda checks:

```python
policies = [
    {"name": "Data Quality Policy", "type": "data_quality"},
    {"name": "Schema Policy", "type": "schema"},
]

validation_result = handler.validate_governance_policies(
    dataset_urn="urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.users,PROD)",
    policies=policies,
    scan_result=scan_result,
)
```

## Supported Platforms

The plugin supports all platforms that Soda Core supports, including:

- PostgreSQL
- Snowflake
- BigQuery
- Redshift
- MySQL
- Microsoft SQL Server
- Oracle
- Databricks
- Spark
- And more...

## Examples

See the `examples/` directory for:

- `example_scan_result.json`: Sample Soda scan result
- `example_usage.py`: Python API usage examples

## Development

### Running Tests

```bash
# Unit tests
pytest tests/unit/

# Integration tests
pytest tests/integration/
```

### Linting

```bash
ruff check src/ tests/
ruff format src/ tests/
```

### Type Checking

```bash
mypy src/ tests/
```

## Integration with Soda Workflows

### Option 1: Post-Scan Hook

Add a post-scan hook in your Soda configuration:

```yaml
# configuration.yml
data_source postgres:
  type: postgres
  connection:
    host: localhost
    username: ${POSTGRES_USERNAME}
    password: ${POSTGRES_PASSWORD}
    database: mydb

# After scan, process results
post_scan:
  - command: datahub-soda --server-url http://localhost:8080 --scan-result scan_result.json
```

### Option 2: Python Script

Create a Python script that runs Soda scans and processes results:

```python
from soda.scan import Scan
from datahub_soda_plugin.handler import DataHubSodaHandler

# Run Soda scan
scan = Scan()
scan.set_data_source_name("postgres")
scan.add_configuration_yaml_file("configuration.yml")
scan.execute()

# Get scan results
scan_result = scan.get_scan_results()

# Send to DataHub
handler = DataHubSodaHandler(server_url="http://localhost:8080")
handler.process_scan_result(scan_result)
```

## Contributing

Contributions are welcome! Please see the main DataHub [contributing guidelines](https://github.com/datahub-project/datahub/blob/master/docs/CONTRIBUTING.md).

## License

Apache 2.0 - See the main DataHub repository for license information.

## Related Documentation

- [DataHub Documentation](https://docs.datahub.com/)
- [Soda Core Documentation](https://docs.soda.io/soda-core/)
- [DataHub Assertions](https://docs.datahub.com/docs/assertions/)
