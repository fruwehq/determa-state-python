"""Closed portable code sets implemented by Determa State Python."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

PORTABLE_CODE_SETS: Mapping[str, frozenset[str]] = MappingProxyType(
    {
        "checkpoint_artifact_failure": frozenset(
            {
                "execution_checkpoint_digest_mismatch",
                "invalid_execution_checkpoint",
                "unsupported_execution_checkpoint_format",
                "unsupported_execution_checkpoint_schema_version",
            }
        ),
        "checkpoint_host_failure": frozenset(
            {
                "checkpoint_revision_conflict",
                "creation_id_conflict",
                "creation_rejected",
                "effect_id_conflict",
                "event_id_conflict",
                "injected_pre_commit_failure",
                "invalid_execution_checkpoint",
                "operation_id_conflict",
                "physical_deletion_unsupported",
                "response_lost_after_commit",
            }
        ),
        "checkpoint_pre_acceptance_failure": frozenset(
            {
                "delivery_digest_mismatch",
                "event_id_conflict",
                "invalid_delivery_mode",
                "invalid_delivery_origin",
                "malformed_delivery",
                "tombstoned_root",
                "wrong_root",
            }
        ),
        "creation_rejection": frozenset(
            {
                "invalid_binding",
                "invalid_creation_request",
                "invalid_machine_target",
            }
        ),
        "dispatch_rejection": frozenset(
            {
                "inactive_component_target",
                "incompatible_bundle",
                "invalid_correlation",
                "invalid_event",
                "invalid_instance_target",
                "invalid_payload",
                "invalid_prior_state",
            }
        ),
        "disposition": frozenset(
            {
                "faulted",
                "handled",
                "rejected",
                "unhandled",
            }
        ),
        "engine_fault": frozenset(
            {
                "action_fault",
                "binding_not_empty",
                "cascade_fault",
                "contained_runtime_fault",
                "guard_fault",
                "inactive_component_target",
                "invalid_instance_target",
                "invariant_fault",
            }
        ),
        "execution_store_adapter_failure": frozenset(
            {
                "adapter_capability_mismatch",
                "duplicate_adapter_registration",
                "invalid_adapter_configuration",
                "unknown_adapter",
            }
        ),
        "machine_load_failure": frozenset(
            {
                "cel_profile_error",
                "destroyed_reference_binding",
                "destroyed_variable_write",
                "duplicate_key",
                "invalid_binding",
                "invalid_boolean_syntax",
                "invalid_null_syntax",
                "invalid_numeric_syntax",
                "invalid_unicode",
                "non_json_value",
                "non_string_map_key",
                "numeric_value_out_of_range",
                "root_local_transition",
                "root_reentry",
                "semantic_validation",
                "unsupported_format",
                "unsupported_yaml_feature",
            }
        ),
        "persistence_failure": frozenset(
            {
                "aggregate_state_digest_mismatch",
                "definition_fingerprint_mismatch",
                "definition_untrusted",
                "invalid_aggregate_state",
                "invalid_aggregate_state_package",
                "invalid_migration_descriptor",
                "invalid_migration_request",
                "migration_descriptor_untrusted",
                "migration_resource_limit_exceeded",
                "migration_route_mismatch",
                "migration_route_missing",
                "migration_totality_failure",
                "migration_transform_fault",
                "source_definition_unavailable",
                "target_definition_unavailable",
                "terminal_migration_rejected",
                "terminal_migration_requires_maintenance",
                "unsupported_aggregate_state_format",
                "unsupported_aggregate_state_package_format",
                "unsupported_aggregate_state_package_schema_version",
                "unsupported_aggregate_state_schema_version",
                "unsupported_migration_descriptor_format",
                "unsupported_migration_descriptor_schema_version",
            }
        ),
    }
)
