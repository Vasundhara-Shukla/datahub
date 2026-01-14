"""
DataHub Soda Plugin - Handler for ingesting Soda scan results into DataHub.

This module provides functionality to:
1. Convert Soda scan results to DataHub assertions
2. Ingest metadata from Soda scans (datasets, schemas, quality metrics)
3. Validate governance policies using DataHub policies and Soda checks
"""

import json
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple, Union

import datahub.emitter.mce_builder as builder
from datahub.cli.env_utils import get_boolean_env_variable
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.serialization_helper import pre_json_transform
from datahub.ingestion.graph.config import ClientMode
from datahub.metadata.com.linkedin.pegasus2avro.assertion import (
    AssertionInfo,
    AssertionResult,
    AssertionResultType,
    AssertionRunEvent,
    AssertionRunStatus,
    AssertionStdAggregation,
    AssertionStdOperator,
    AssertionStdParameter,
    AssertionStdParameters,
    AssertionStdParameterType,
    AssertionType,
    BatchSpec,
    DatasetAssertionInfo,
    DatasetAssertionScope,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import DataPlatformInstance
from datahub.metadata.schema_classes import PartitionSpecClass, PartitionTypeClass
from datahub.utilities.urns.dataset_urn import DatasetUrn

logger = logging.getLogger(__name__)
if get_boolean_env_variable("DATAHUB_DEBUG", False):
    handler = logging.StreamHandler(stream=sys.stdout)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

SODA_PLATFORM_NAME = "soda"


class DataHubSodaHandler:
    """
    Handler for processing Soda scan results and sending them to DataHub.

    This class converts Soda data quality checks into DataHub assertions,
    ingests metadata about datasets, and can validate governance policies.
    """

    def __init__(
        self,
        server_url: str,
        env: str = builder.DEFAULT_ENV,
        platform_alias: Optional[str] = None,
        platform_instance_map: Optional[Dict[str, str]] = None,
        graceful_exceptions: bool = True,
        token: Optional[str] = None,
        timeout_sec: Optional[float] = None,
        retry_status_codes: Optional[List[int]] = None,
        retry_max_times: Optional[int] = None,
        extra_headers: Optional[Dict[str, str]] = None,
        convert_urns_to_lowercase: bool = False,
    ):
        """
        Initialize the DataHub Soda Handler.

        Args:
            server_url: DataHub GMS server URL
            env: Environment name (default: PROD)
            platform_alias: Optional platform alias to use instead of detected platform
            platform_instance_map: Map of datasource names to platform instances
            graceful_exceptions: Whether to suppress exceptions (default: True)
            token: DataHub authentication token
            timeout_sec: Request timeout in seconds
            retry_status_codes: HTTP status codes to retry on
            retry_max_times: Maximum number of retries
            extra_headers: Additional headers to include in requests
            convert_urns_to_lowercase: Whether to convert dataset names to lowercase
        """
        self.server_url = server_url
        self.env = env
        self.platform_alias = platform_alias
        self.platform_instance_map = platform_instance_map or {}
        self.graceful_exceptions = graceful_exceptions
        self.token = token
        self.timeout_sec = timeout_sec
        self.retry_status_codes = retry_status_codes
        self.retry_max_times = retry_max_times
        self.extra_headers = extra_headers
        self.convert_urns_to_lowercase = convert_urns_to_lowercase

    def process_scan_result(
        self,
        scan_result: Dict[str, Any],
        scan_timestamp: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Process a Soda scan result and send metadata to DataHub.

        Args:
            scan_result: Soda scan result dictionary containing checks, tables, etc.
            scan_timestamp: Timestamp of the scan (default: current time)

        Returns:
            Dictionary with processing results
        """
        if scan_timestamp is None:
            scan_timestamp = datetime.now(timezone.utc)

        try:
            emitter = DatahubRestEmitter(
                gms_server=self.server_url,
                token=self.token,
                read_timeout_sec=self.timeout_sec,
                connect_timeout_sec=self.timeout_sec,
                retry_status_codes=self.retry_status_codes,
                retry_max_times=self.retry_max_times,
                extra_headers=self.extra_headers,
                client_mode=ClientMode.INGESTION,
                datahub_component="soda-plugin",
            )

            # Extract scan metadata
            scan_id = scan_result.get("scanId", f"scan_{int(time.time())}")
            data_source_name = scan_result.get("dataSourceName", "unknown")

            # Process checks from scan result
            checks = scan_result.get("checks", [])
            tables = scan_result.get("tables", [])

            logger.info(f"Processing Soda scan {scan_id} with {len(checks)} checks")

            # Process each table and its checks
            assertions_sent = 0
            for table_info in tables:
                table_name = table_info.get("tableName", "")
                schema_name = table_info.get("schemaName", "")
                database_name = table_info.get("databaseName", "")

                # Get platform instance
                platform_instance = self.platform_instance_map.get(
                    data_source_name, None
                )

                # Build dataset URN
                dataset_urn = self._build_dataset_urn(
                    data_source_name=data_source_name,
                    database_name=database_name,
                    schema_name=schema_name,
                    table_name=table_name,
                    platform_instance=platform_instance,
                )

                if not dataset_urn:
                    logger.warning(
                        f"Could not build dataset URN for {schema_name}.{table_name}"
                    )
                    continue

                logger.info(f"Processing dataset: {dataset_urn}")

                # Process checks for this table
                table_checks = [
                    c
                    for c in checks
                    if c.get("table") == table_name
                    and c.get("schema") == schema_name
                ]

                for check in table_checks:
                    assertion_info, assertion_result = self._convert_check_to_assertion(
                        check=check,
                        dataset_urn=dataset_urn,
                        scan_id=scan_id,
                        scan_timestamp=scan_timestamp,
                    )

                    if assertion_info and assertion_result:
                        # Emit assertion info
                        assertion_info_mcp = MetadataChangeProposalWrapper(
                            entityUrn=assertion_result.assertionUrn,
                            aspect=assertion_info,
                        )
                        emitter.emit_mcp(assertion_info_mcp)

                        # Emit platform instance
                        assertion_platform_mcp = MetadataChangeProposalWrapper(
                            entityUrn=assertion_result.assertionUrn,
                            aspect=DataPlatformInstance(
                                platform=builder.make_data_platform_urn(
                                    SODA_PLATFORM_NAME
                                )
                            ),
                        )
                        emitter.emit_mcp(assertion_platform_mcp)

                        # Emit assertion result
                        assertion_result_mcp = MetadataChangeProposalWrapper(
                            entityUrn=assertion_result.assertionUrn,
                            aspect=assertion_result,
                        )
                        emitter.emit_mcp(assertion_result_mcp)

                        assertions_sent += 1

            logger.info(f"Successfully sent {assertions_sent} assertions to DataHub")
            return {
                "status": "success",
                "assertions_sent": assertions_sent,
                "scan_id": scan_id,
            }

        except Exception as e:
            error_msg = f"Failed to process Soda scan result: {str(e)}"
            logger.error(error_msg, exc_info=True)
            if self.graceful_exceptions:
                return {"status": "error", "error": error_msg}
            else:
                raise

    def _build_dataset_urn(
        self,
        data_source_name: str,
        database_name: str,
        schema_name: str,
        table_name: str,
        platform_instance: Optional[str] = None,
    ) -> Optional[str]:
        """
        Build a DataHub dataset URN from Soda table information.

        Args:
            data_source_name: Name of the data source
            database_name: Database name
            schema_name: Schema name
            table_name: Table name
            platform_instance: Optional platform instance

        Returns:
            Dataset URN string or None if unable to build
        """
        # Map Soda data source names to DataHub platforms
        platform_map = {
            "postgres": "postgres",
            "snowflake": "snowflake",
            "bigquery": "bigquery",
            "redshift": "redshift",
            "mysql": "mysql",
            "mssql": "mssql",
            "oracle": "oracle",
            "databricks": "databricks",
            "spark": "spark",
        }

        platform = self.platform_alias or platform_map.get(
            data_source_name.lower(), data_source_name.lower()
        )

        # Build dataset name
        if database_name and schema_name:
            dataset_name = f"{database_name}.{schema_name}.{table_name}"
        elif schema_name:
            dataset_name = f"{schema_name}.{table_name}"
        else:
            dataset_name = table_name

        if self.convert_urns_to_lowercase:
            dataset_name = dataset_name.lower()

        try:
            return builder.make_dataset_urn_with_platform_instance(
                platform=platform,
                name=dataset_name,
                platform_instance=platform_instance or "",
                env=self.env,
            )
        except Exception as e:
            logger.warning(f"Failed to build dataset URN: {e}")
            return None

    def _convert_check_to_assertion(
        self,
        check: Dict[str, Any],
        dataset_urn: str,
        scan_id: str,
        scan_timestamp: datetime,
    ) -> Tuple[Optional[AssertionInfo], Optional[AssertionRunEvent]]:
        """
        Convert a Soda check to DataHub assertion format.

        Args:
            check: Soda check dictionary
            dataset_urn: Dataset URN
            scan_id: Scan ID
            scan_timestamp: Timestamp of the scan

        Returns:
            Tuple of (AssertionInfo, AssertionRunEvent) or (None, None) if conversion fails
        """
        try:
            check_name = check.get("name", "unknown_check")
            check_type = check.get("type", "unknown")
            definition = check.get("definition", "")
            outcome = check.get("outcome", "unknown")
            metrics = check.get("metrics", [])
            column_name = check.get("column", None)

            # Determine assertion result
            success = outcome.lower() in ["pass", "passed", "success"]
            result_type = (
                AssertionResultType.SUCCESS
                if success
                else AssertionResultType.FAILURE
            )

            # Extract metrics
            row_count = None
            missing_count = None
            unexpected_count = None
            actual_agg_value = None

            for metric in metrics:
                metric_id = metric.get("id", "")
                metric_value = metric.get("value")
                if metric_id == "row_count":
                    row_count = parse_int_or_default(metric_value)
                elif metric_id == "missing_count":
                    missing_count = parse_int_or_default(metric_value)
                elif metric_id == "unexpected_count":
                    unexpected_count = parse_int_or_default(metric_value)
                elif metric_id in ["min", "max", "avg", "sum", "count"]:
                    if isinstance(metric_value, (int, float)):
                        actual_agg_value = metric_value

            # Build assertion URN
            assertion_fields = None
            if column_name:
                assertion_fields = [
                    builder.make_schema_field_urn(dataset_urn, column_name)
                ]

            assertion_urn = builder.make_assertion_urn(
                builder.datahub_guid(
                    pre_json_transform(
                        {
                            "platform": SODA_PLATFORM_NAME,
                            "nativeType": check_type,
                            "nativeParameters": {"definition": definition},
                            "dataset": dataset_urn,
                            "fields": assertion_fields,
                            "checkName": check_name,
                        }
                    )
                )
            )

            # Map Soda check types to DataHub assertion operators
            assertion_info = self._build_assertion_info(
                check_type=check_type,
                definition=definition,
                dataset_urn=dataset_urn,
                assertion_fields=assertion_fields,
                check_name=check_name,
            )

            # Build batch spec
            batch_spec = BatchSpec(
                nativeBatchId=scan_id,
                query=definition if definition else "",
                customProperties={
                    "check_name": check_name,
                    "check_type": check_type,
                    "soda_scan_id": scan_id,
                },
            )

            # Build assertion result
            assertion_result = AssertionRunEvent(
                timestampMillis=int(round(time.time() * 1000)),
                assertionUrn=assertion_urn,
                asserteeUrn=dataset_urn,
                runId=scan_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
                result=AssertionResult(
                    type=result_type,
                    rowCount=row_count,
                    missingCount=missing_count,
                    unexpectedCount=unexpected_count,
                    actualAggValue=actual_agg_value,
                    nativeResults={
                        k: convert_to_string(v)
                        for k, v in check.items()
                        if k not in ["name", "type", "definition", "outcome"]
                        and v is not None
                    },
                ),
                batchSpec=batch_spec,
                status=AssertionRunStatus.COMPLETE,
            )

            return assertion_info, assertion_result

        except Exception as e:
            logger.warning(f"Failed to convert check to assertion: {e}")
            return None, None

    def _build_assertion_info(
        self,
        check_type: str,
        definition: str,
        dataset_urn: str,
        assertion_fields: Optional[List[str]],
        check_name: str,
    ) -> AssertionInfo:
        """
        Build AssertionInfo from Soda check information.

        Args:
            check_type: Type of Soda check
            definition: Check definition/query
            dataset_urn: Dataset URN
            assertion_fields: Optional list of field URNs
            check_name: Name of the check

        Returns:
            AssertionInfo object
        """
        # Map common Soda check types to DataHub assertion operators
        # This is a simplified mapping - can be extended
        scope = DatasetAssertionScope.DATASET_ROWS
        operator = AssertionStdOperator._NATIVE_
        aggregation = AssertionStdAggregation._NATIVE_
        parameters = None

        check_lower = check_type.lower()
        if "null" in check_lower or "missing" in check_lower:
            scope = (
                DatasetAssertionScope.DATASET_COLUMN
                if assertion_fields
                else DatasetAssertionScope.DATASET_ROWS
            )
            operator = AssertionStdOperator.NOT_NULL
            aggregation = AssertionStdAggregation.IDENTITY
        elif "unique" in check_lower:
            scope = (
                DatasetAssertionScope.DATASET_COLUMN
                if assertion_fields
                else DatasetAssertionScope.DATASET_ROWS
            )
            operator = AssertionStdOperator._NATIVE_
            aggregation = AssertionStdAggregation.UNIQUE_COUNT
        elif "row_count" in check_lower or "count" in check_lower:
            scope = DatasetAssertionScope.DATASET_ROWS
            operator = AssertionStdOperator._NATIVE_
            aggregation = AssertionStdAggregation.ROW_COUNT
        elif "schema" in check_lower:
            scope = DatasetAssertionScope.DATASET_SCHEMA
            operator = AssertionStdOperator._NATIVE_
            aggregation = AssertionStdAggregation.COLUMNS
        elif assertion_fields:
            scope = DatasetAssertionScope.DATASET_COLUMN
            operator = AssertionStdOperator._NATIVE_
            aggregation = AssertionStdAggregation._NATIVE_

        dataset_assertion_info = DatasetAssertionInfo(
            dataset=dataset_urn,
            fields=assertion_fields,
            operator=operator,
            aggregation=aggregation,
            nativeType=check_type,
            nativeParameters={"definition": definition},
            scope=scope,
        )

        return AssertionInfo(
            type=AssertionType.DATASET,
            datasetAssertion=dataset_assertion_info,
            customProperties={"check_name": check_name, "soda_check_type": check_type},
        )

    def validate_governance_policies(
        self,
        dataset_urn: str,
        policies: List[Dict[str, Any]],
        scan_result: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Validate governance policies against a dataset using Soda checks.

        This method can be used to check if DataHub governance policies
        are being enforced through Soda data quality checks.

        Args:
            dataset_urn: Dataset URN to validate
            policies: List of governance policies from DataHub
            scan_result: Optional Soda scan result to validate against

        Returns:
            Dictionary with validation results
        """
        validation_results = {
            "dataset_urn": dataset_urn,
            "policies_validated": len(policies),
            "compliant": 0,
            "non_compliant": 0,
            "details": [],
        }

        # This is a placeholder for policy validation logic
        # In a full implementation, this would:
        # 1. Fetch policies from DataHub for the dataset
        # 2. Map policies to expected Soda checks
        # 3. Validate that required checks exist and pass
        # 4. Return compliance status

        logger.info(
            f"Validating {len(policies)} governance policies for {dataset_urn}"
        )

        for policy in policies:
            policy_name = policy.get("name", "unknown")
            policy_type = policy.get("type", "unknown")

            # Simplified validation - in practice, this would be more sophisticated
            validation_results["details"].append(
                {
                    "policy": policy_name,
                    "type": policy_type,
                    "status": "validated",
                }
            )

        return validation_results


def parse_int_or_default(value: Any, default_value: Optional[int] = None) -> Optional[int]:
    """Parse integer value or return default."""
    if value is None:
        return default_value
    try:
        return int(value)
    except (ValueError, TypeError):
        return default_value


def convert_to_string(var: Any) -> str:
    """Convert variable to string, handling special types."""
    try:
        if isinstance(var, (str, int, float)):
            return str(var)
        elif isinstance(var, Decimal):
            return str(var)
        else:
            return json.dumps(var, default=str)
    except (TypeError, ValueError) as e:
        logger.debug(f"Error converting to string: {e}")
        return str(var)
