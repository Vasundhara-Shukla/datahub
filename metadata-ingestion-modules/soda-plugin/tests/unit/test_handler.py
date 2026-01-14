"""Unit tests for DataHub Soda Handler."""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from datahub_soda_plugin.handler import DataHubSodaHandler


@pytest.fixture
def sample_scan_result():
    """Sample Soda scan result for testing."""
    return {
        "scanId": "test_scan_123",
        "dataSourceName": "postgres",
        "tables": [
            {
                "tableName": "users",
                "schemaName": "public",
                "databaseName": "mydb",
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
                    {"id": "missing_count", "value": 0},
                ],
            },
            {
                "name": "users_email_unique",
                "type": "duplicate_count",
                "definition": "SELECT COUNT(*) FROM (SELECT email, COUNT(*) FROM users GROUP BY email HAVING COUNT(*) > 1)",
                "outcome": "fail",
                "table": "users",
                "schema": "public",
                "column": "email",
                "metrics": [
                    {"id": "row_count", "value": 1000},
                    {"id": "unexpected_count", "value": 5},
                ],
            },
        ],
    }


@pytest.fixture
def handler():
    """Create a DataHubSodaHandler instance for testing."""
    return DataHubSodaHandler(
        server_url="http://localhost:8080",
        env="TEST",
        graceful_exceptions=True,
    )


def test_build_dataset_urn(handler):
    """Test building dataset URN from table information."""
    urn = handler._build_dataset_urn(
        data_source_name="postgres",
        database_name="mydb",
        schema_name="public",
        table_name="users",
    )
    assert urn is not None
    assert "mydb.public.users" in urn or "postgres" in urn.lower()


def test_build_dataset_urn_with_platform_instance(handler):
    """Test building dataset URN with platform instance."""
    handler.platform_instance_map = {"postgres": "prod_postgres"}
    urn = handler._build_dataset_urn(
        data_source_name="postgres",
        database_name="mydb",
        schema_name="public",
        table_name="users",
        platform_instance="prod_postgres",
    )
    assert urn is not None


def test_convert_check_to_assertion(handler, sample_scan_result):
    """Test converting Soda check to DataHub assertion."""
    check = sample_scan_result["checks"][0]
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.users,TEST)"

    assertion_info, assertion_result = handler._convert_check_to_assertion(
        check=check,
        dataset_urn=dataset_urn,
        scan_id="test_scan_123",
        scan_timestamp=datetime.now(timezone.utc),
    )

    assert assertion_info is not None
    assert assertion_result is not None
    assert assertion_result.result.type.value == "SUCCESS"  # pass outcome


def test_convert_check_to_assertion_failure(handler, sample_scan_result):
    """Test converting failed Soda check to DataHub assertion."""
    check = sample_scan_result["checks"][1]  # This one fails
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.users,TEST)"

    assertion_info, assertion_result = handler._convert_check_to_assertion(
        check=check,
        dataset_urn=dataset_urn,
        scan_id="test_scan_123",
        scan_timestamp=datetime.now(timezone.utc),
    )

    assert assertion_info is not None
    assert assertion_result is not None
    assert assertion_result.result.type.value == "FAILURE"  # fail outcome


@patch("datahub_soda_plugin.handler.DatahubRestEmitter")
def test_process_scan_result(mock_emitter_class, handler, sample_scan_result):
    """Test processing a complete scan result."""
    mock_emitter = MagicMock()
    mock_emitter_class.return_value = mock_emitter

    result = handler.process_scan_result(sample_scan_result)

    assert result["status"] == "success"
    assert result["assertions_sent"] == 2
    assert mock_emitter.emit_mcp.called


def test_build_assertion_info_null_check(handler):
    """Test building assertion info for null check."""
    assertion_info = handler._build_assertion_info(
        check_type="missing_count",
        definition="SELECT COUNT(*) FROM users WHERE id IS NULL",
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.users,TEST)",
        assertion_fields=["urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.users,TEST),id)"],
        check_name="users_id_not_null",
    )

    assert assertion_info.type.value == "DATASET"
    assert assertion_info.datasetAssertion.scope.value == "DATASET_COLUMN"


def test_validate_governance_policies(handler):
    """Test governance policy validation."""
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.users,TEST)"
    policies = [
        {"name": "Data Quality Policy", "type": "data_quality"},
        {"name": "Schema Policy", "type": "schema"},
    ]

    result = handler.validate_governance_policies(
        dataset_urn=dataset_urn,
        policies=policies,
    )

    assert result["dataset_urn"] == dataset_urn
    assert result["policies_validated"] == 2
    assert len(result["details"]) == 2
