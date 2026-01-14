# Running the DataHub Soda Plugin Locally

Quick guide to get up and running in 5 minutes.

## Prerequisites Check

Run the test script to verify everything is set up:

```bash
cd metadata-ingestion-modules/soda-plugin
./test_local.sh
```

## Quick Start (3 Steps)

### Step 1: Start DataHub (if not running)

```bash
# Option A: Docker (recommended for testing)
docker run -d \
  --name datahub \
  -p 8080:8080 \
  -p 8081:8081 \
  -p 9002:9002 \
  acryldata/datahub-upgrade:headless -n

# Wait for it to start (about 30 seconds)
sleep 30

# Verify it's running
curl http://localhost:8080/health
```

**Or** use your existing DataHub instance.

### Step 2: Install the Plugin

```bash
cd metadata-ingestion-modules/soda-plugin

# Create virtual environment (optional but recommended)
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install
pip install -e .
```

### Step 3: Run the Demo

```bash
# Option A: Use the demo script (recommended)
python3 examples/demo_workflow.py

# Option B: Use CLI
datahub-soda \
  --server-url http://localhost:8080 \
  --scan-result examples/example_scan_result.json \
  --env PROD

# Option C: Use Python API
python3 examples/example_usage.py
```

## Verify Results

1. Open DataHub UI: http://localhost:9002
2. Search for: `mydb.public.users` or `mydb.public.orders`
3. Click on the dataset
4. Go to the "Assertions" tab
5. You should see the Soda checks!

## Common Issues

### "Connection refused" error

```bash
# Check if DataHub is running
curl http://localhost:8080/health

# If not, start it:
docker run -p 8080:8080 acryldata/datahub-upgrade:headless -n
```

### "Module not found" error

```bash
# Make sure you're in the plugin directory
cd metadata-ingestion-modules/soda-plugin

# Reinstall
pip install -e . --force-reinstall
```

### "Dataset not found" in DataHub

The dataset needs to exist in DataHub first. You can:
1. Ingest the dataset metadata first using DataHub's ingestion framework
2. Or create it manually in the UI
3. The assertions will still be created and linked when the dataset is ingested

## Testing Different Scenarios

### Test 1: Basic Scan Result

```bash
datahub-soda \
  --server-url http://localhost:8080 \
  --scan-result examples/example_scan_result.json
```

### Test 2: With Authentication

```bash
# Get token from DataHub UI: Settings → Access Tokens
datahub-soda \
  --server-url http://localhost:8080 \
  --token YOUR_TOKEN \
  --scan-result examples/example_scan_result.json
```

### Test 3: With Platform Instance Mapping

Create `platform_map.json`:
```json
{
  "postgres": "prod_postgres_instance",
  "snowflake": "analytics_warehouse"
}
```

Then:
```bash
datahub-soda \
  --server-url http://localhost:8080 \
  --scan-result examples/example_scan_result.json \
  --platform-instance-map platform_map.json
```

### Test 4: Python API

```python
from datahub_soda_plugin.handler import DataHubSodaHandler
import json

handler = DataHubSodaHandler(server_url="http://localhost:8080")

with open("examples/example_scan_result.json") as f:
    result = handler.process_scan_result(json.load(f))
    
print(f"Sent {result['assertions_sent']} assertions")
```

## For Blog/Screenshots

### Recommended Demo Flow

1. **Before**: Show DataHub without quality data
   - Open dataset page
   - Show empty assertions tab

2. **Run Plugin**: Execute the demo
   ```bash
   python3 examples/demo_workflow.py
   ```

3. **After**: Show DataHub with quality data
   - Refresh dataset page
   - Show populated assertions tab
   - Show check results and metrics

4. **Governance**: Show policy validation
   - Run governance validation code
   - Show compliance results

### Screenshot Checklist

- [ ] DataHub login/homepage
- [ ] Dataset search (before)
- [ ] Dataset page - empty assertions tab
- [ ] Terminal showing plugin execution
- [ ] Dataset page - populated assertions tab
- [ ] Assertion details view
- [ ] Governance validation results

## Next Steps

- Read `QUICKSTART.md` for detailed setup
- Read `BLOG_GUIDE.md` for blog content ideas
- Check `README.md` for full documentation
- Explore `examples/` for more use cases
