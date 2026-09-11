"""Closed portable code sets implemented by Determa State Python."""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from types import MappingProxyType


class CheckpointArtifactFailureCode(StrEnum):
    EXECUTION_CHECKPOINT_DIGEST_MISMATCH = "execution_checkpoint_digest_mismatch"
    INVALID_EXECUTION_CHECKPOINT = "invalid_execution_checkpoint"
    UNSUPPORTED_EXECUTION_CHECKPOINT_FORMAT = "unsupported_execution_checkpoint_format"
    UNSUPPORTED_EXECUTION_CHECKPOINT_SCHEMA_VERSION = (
        "unsupported_execution_checkpoint_schema_version"
    )


class CheckpointHostFailureCode(StrEnum):
    CHECKPOINT_REVISION_CONFLICT = "checkpoint_revision_conflict"
    CREATION_ID_CONFLICT = "creation_id_conflict"
    CREATION_REJECTED = "creation_rejected"
    EFFECT_ID_CONFLICT = "effect_id_conflict"
    EVENT_ID_CONFLICT = "event_id_conflict"
    INJECTED_PRE_COMMIT_FAILURE = "injected_pre_commit_failure"
    INVALID_EXECUTION_CHECKPOINT = "invalid_execution_checkpoint"
    OPERATION_ID_CONFLICT = "operation_id_conflict"
    PHYSICAL_DELETION_UNSUPPORTED = "physical_deletion_unsupported"
    RESPONSE_LOST_AFTER_COMMIT = "response_lost_after_commit"


class CheckpointPreAcceptanceFailureCode(StrEnum):
    CHECKPOINT_UPGRADE_REQUIRED = "checkpoint_upgrade_required"
    DELIVERY_DIGEST_MISMATCH = "delivery_digest_mismatch"
    DUPLICATE_EVENT_ID_IN_BATCH = "duplicate_event_id_in_batch"
    EVENT_ID_CONFLICT = "event_id_conflict"
    INACTIVE_COMPONENT_TARGET = "inactive_component_target"
    INVALID_CORRELATION = "invalid_correlation"
    INVALID_DELIVERY_MODE = "invalid_delivery_mode"
    INVALID_DELIVERY_ORIGIN = "invalid_delivery_origin"
    INVALID_DELIVERY_SOURCE = "invalid_delivery_source"
    INVALID_EVENT = "invalid_event"
    INVALID_INSTANCE_TARGET = "invalid_instance_target"
    INVALID_PAYLOAD = "invalid_payload"
    MALFORMED_DELIVERY = "malformed_delivery"
    TERMINAL_ROOT = "terminal_root"
    TOMBSTONED_ROOT = "tombstoned_root"
    WRONG_ROOT = "wrong_root"


class CreationRejectionCode(StrEnum):
    INVALID_BINDING = "invalid_binding"
    INVALID_CREATION_REQUEST = "invalid_creation_request"
    INVALID_MACHINE_TARGET = "invalid_machine_target"


class DispatchRejectionCode(StrEnum):
    INACTIVE_COMPONENT_TARGET = "inactive_component_target"
    INCOMPATIBLE_BUNDLE = "incompatible_bundle"
    INVALID_CORRELATION = "invalid_correlation"
    INVALID_EVENT = "invalid_event"
    INVALID_INSTANCE_TARGET = "invalid_instance_target"
    INVALID_PAYLOAD = "invalid_payload"
    INVALID_PRIOR_STATE = "invalid_prior_state"


class DispositionCode(StrEnum):
    DEFERRED = "deferred"
    FAULTED = "faulted"
    HANDLED = "handled"
    NOT_RUNNABLE = "not_runnable"
    REJECTED = "rejected"
    UNHANDLED = "unhandled"


class EngineFaultCode(StrEnum):
    ACTION_FAULT = "action_fault"
    BINDING_NOT_EMPTY = "binding_not_empty"
    CASCADE_FAULT = "cascade_fault"
    CONTAINED_RUNTIME_FAULT = "contained_runtime_fault"
    DEFERRED_EVENT_CAPACITY_EXCEEDED = "deferred_event_capacity_exceeded"
    GUARD_FAULT = "guard_fault"
    INACTIVE_COMPONENT_TARGET = "inactive_component_target"
    INVALID_INSTANCE_TARGET = "invalid_instance_target"
    INVARIANT_FAULT = "invariant_fault"


class ExecutionStoreAdapterFailureCode(StrEnum):
    ADAPTER_CAPABILITY_MISMATCH = "adapter_capability_mismatch"
    DUPLICATE_ADAPTER_REGISTRATION = "duplicate_adapter_registration"
    INVALID_ADAPTER_CONFIGURATION = "invalid_adapter_configuration"
    UNKNOWN_ADAPTER = "unknown_adapter"


class MachineLoadFailureCode(StrEnum):
    CEL_PROFILE_ERROR = "cel_profile_error"
    DESTROYED_REFERENCE_BINDING = "destroyed_reference_binding"
    DESTROYED_VARIABLE_WRITE = "destroyed_variable_write"
    DUPLICATE_KEY = "duplicate_key"
    INVALID_BINDING = "invalid_binding"
    INVALID_BOOLEAN_SYNTAX = "invalid_boolean_syntax"
    INVALID_NULL_SYNTAX = "invalid_null_syntax"
    INVALID_NUMERIC_SYNTAX = "invalid_numeric_syntax"
    INVALID_UNICODE = "invalid_unicode"
    NON_JSON_VALUE = "non_json_value"
    NON_STRING_MAP_KEY = "non_string_map_key"
    NUMERIC_VALUE_OUT_OF_RANGE = "numeric_value_out_of_range"
    ROOT_LOCAL_TRANSITION = "root_local_transition"
    ROOT_REENTRY = "root_reentry"
    SEMANTIC_VALIDATION = "semantic_validation"
    UNSUPPORTED_FORMAT = "unsupported_format"
    UNSUPPORTED_YAML_FEATURE = "unsupported_yaml_feature"


class PersistenceFailureCode(StrEnum):
    AGGREGATE_STATE_DIGEST_MISMATCH = "aggregate_state_digest_mismatch"
    DEFINITION_FINGERPRINT_MISMATCH = "definition_fingerprint_mismatch"
    DEFINITION_UNTRUSTED = "definition_untrusted"
    INVALID_AGGREGATE_STATE = "invalid_aggregate_state"
    INVALID_AGGREGATE_STATE_PACKAGE = "invalid_aggregate_state_package"
    INVALID_MIGRATION_DESCRIPTOR = "invalid_migration_descriptor"
    INVALID_MIGRATION_REQUEST = "invalid_migration_request"
    MIGRATION_DESCRIPTOR_UNTRUSTED = "migration_descriptor_untrusted"
    MIGRATION_RESOURCE_LIMIT_EXCEEDED = "migration_resource_limit_exceeded"
    MIGRATION_ROUTE_MISMATCH = "migration_route_mismatch"
    MIGRATION_ROUTE_MISSING = "migration_route_missing"
    MIGRATION_TOTALITY_FAILURE = "migration_totality_failure"
    MIGRATION_TRANSFORM_FAULT = "migration_transform_fault"
    SOURCE_DEFINITION_UNAVAILABLE = "source_definition_unavailable"
    TARGET_DEFINITION_UNAVAILABLE = "target_definition_unavailable"
    TERMINAL_MIGRATION_REJECTED = "terminal_migration_rejected"
    TERMINAL_MIGRATION_REQUIRES_MAINTENANCE = "terminal_migration_requires_maintenance"
    UNSUPPORTED_AGGREGATE_STATE_FORMAT = "unsupported_aggregate_state_format"
    UNSUPPORTED_AGGREGATE_STATE_PACKAGE_FORMAT = "unsupported_aggregate_state_package_format"
    UNSUPPORTED_AGGREGATE_STATE_PACKAGE_SCHEMA_VERSION = (
        "unsupported_aggregate_state_package_schema_version"
    )
    UNSUPPORTED_AGGREGATE_STATE_SCHEMA_VERSION = "unsupported_aggregate_state_schema_version"
    UNSUPPORTED_MIGRATION_DESCRIPTOR_FORMAT = "unsupported_migration_descriptor_format"
    UNSUPPORTED_MIGRATION_DESCRIPTOR_SCHEMA_VERSION = (
        "unsupported_migration_descriptor_schema_version"
    )


_CATEGORY_TYPES: Mapping[str, type[StrEnum]] = MappingProxyType(
    {
        "checkpoint_artifact_failure": CheckpointArtifactFailureCode,
        "checkpoint_host_failure": CheckpointHostFailureCode,
        "checkpoint_pre_acceptance_failure": CheckpointPreAcceptanceFailureCode,
        "creation_rejection": CreationRejectionCode,
        "dispatch_rejection": DispatchRejectionCode,
        "disposition": DispositionCode,
        "engine_fault": EngineFaultCode,
        "execution_store_adapter_failure": ExecutionStoreAdapterFailureCode,
        "machine_load_failure": MachineLoadFailureCode,
        "persistence_failure": PersistenceFailureCode,
    }
)

PORTABLE_CODE_SETS: Mapping[str, frozenset[str]] = MappingProxyType(
    {
        category: frozenset(code.value for code in code_type)
        for category, code_type in _CATEGORY_TYPES.items()
    }
)
