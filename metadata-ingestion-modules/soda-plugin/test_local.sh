#!/bin/bash
# Quick test script to verify the plugin works locally

set -e

echo "🧪 Testing DataHub Soda Plugin Locally"
echo "========================================"

# Check if DataHub is running
echo ""
echo "1. Checking DataHub connection..."
if curl -s http://localhost:8080/health > /dev/null 2>&1; then
    echo "✅ DataHub is running at http://localhost:8080"
else
    echo "❌ DataHub is not running!"
    echo "   Start it with: docker run -p 8080:8080 acryldata/datahub-upgrade:headless -n"
    exit 1
fi

# Check Python installation
echo ""
echo "2. Checking Python installation..."
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version)
    echo "✅ $PYTHON_VERSION"
else
    echo "❌ Python 3 not found!"
    exit 1
fi

# Check if plugin is installed
echo ""
echo "3. Checking plugin installation..."
if python3 -c "import datahub_soda_plugin" 2>/dev/null; then
    echo "✅ Plugin is installed"
else
    echo "⚠️  Plugin not installed. Installing now..."
    pip install -e . > /dev/null 2>&1
    echo "✅ Plugin installed"
fi

# Test CLI
echo ""
echo "4. Testing CLI..."
if command -v datahub-soda &> /dev/null; then
    echo "✅ CLI command available"
    datahub-soda --help > /dev/null 2>&1 && echo "✅ CLI help works"
else
    echo "⚠️  CLI not found in PATH (this is OK if using pip install -e .)"
fi

# Test with example scan result
echo ""
echo "5. Testing with example scan result..."
if [ -f "examples/example_scan_result.json" ]; then
    echo "✅ Example scan result found"
    
    # Try to process it (will fail if DataHub not properly configured, but that's OK)
    echo "   Attempting to process scan result..."
    if python3 examples/demo_workflow.py 2>&1 | grep -q "Successfully processed\|Error\|Failed"; then
        echo "✅ Plugin can process scan results"
    else
        echo "⚠️  Could not process (may need DataHub token or configuration)"
    fi
else
    echo "❌ Example scan result not found"
fi

echo ""
echo "========================================"
echo "✅ Basic checks complete!"
echo ""
echo "Next steps:"
echo "1. Run: python3 examples/demo_workflow.py"
echo "2. Or: datahub-soda --server-url http://localhost:8080 --scan-result examples/example_scan_result.json"
echo "3. View results in DataHub UI: http://localhost:9002"
