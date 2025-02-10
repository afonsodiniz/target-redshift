"""Tests standard target features using the built-in SDK tests library."""

from __future__ import annotations

import json
import typing as t

from singer_sdk.testing import get_target_test_class
from singer_sdk.testing.suites import TestSuite
from singer_sdk.testing.target_tests import (
    TargetArrayData,
    TargetCamelcaseComplexSchema,
    TargetCamelcaseTest,
    TargetCliPrintsTest,
    TargetDuplicateRecords,
    TargetInvalidSchemaTest,
    TargetNoPrimaryKeys,
    TargetOptionalAttributes,
    TargetRecordBeforeSchemaTest,
    TargetRecordMissingKeyProperty,
    TargetRecordMissingOptionalFields,
    TargetSchemaNoProperties,
    TargetSchemaUpdates,
    TargetSpecialCharsInAttributes,
)

from target_redshift.target import TargetRedshift

with open(".secrets/config.json") as f:  # noqa: PTH123
    redshift_config: dict[str, t.Any] = json.load(f)

target_tests = TestSuite(
    kind="target",
    tests=[
        TargetArrayData,
        TargetCamelcaseComplexSchema,
        TargetCamelcaseTest,
        TargetCliPrintsTest,
        TargetDuplicateRecords,
        TargetInvalidSchemaTest,
        TargetNoPrimaryKeys,
        TargetOptionalAttributes,
        TargetRecordBeforeSchemaTest,
        TargetRecordMissingKeyProperty,
        TargetSchemaNoProperties,
        TargetSchemaUpdates,
        TargetSpecialCharsInAttributes,
        TargetRecordMissingOptionalFields,
    ],
)

# Run standard built-in target tests from the SDK:
StandardTargetTests = get_target_test_class(
    target_class=TargetRedshift,
    config=redshift_config,
    custom_suites=[target_tests],
    suite_config=None,
    include_target_tests=False,
)


class TestTargetRedshift(StandardTargetTests):  # type: ignore[misc, valid-type]
    """Standard Target Tests."""
