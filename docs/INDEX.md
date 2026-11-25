# Documentation Index

This index is the entry point for project documentation. Use the readiness and operational documents as the source of truth for current status; dated execution reports are evidence snapshots and may be superseded by later reviews.

## Start here

| Document | Purpose |
|---|---|
| [Platform Guide](PLATFORM_GUIDE.md) | Full local setup, service behavior, data contracts, dashboards and development workflows |
| [Azure Readiness Review](AZURE_READINESS_REVIEW_2026-11-25.md) | Current Azure gate assessment and deployment blockers |
| [Azure Remaining Gates](AZURE_REMAINING_GATES_2026-11-24.md) | Outstanding technical and operational acceptance criteria |
| [Azure Streaming Architecture Decision](AZURE_STREAMING_ARCHITECTURE_DECISION_2026-11-24.md) | Candidate cloud architecture and compatibility decisions |
| [Operations Recovery and Backup Runbook](OPERATIONS_RECOVERY_AND_BACKUP_RUNBOOK.md) | Backup, restore and recovery procedures |
| [Security and Recovery Remediation](SECURITY_AND_RECOVERY_REMEDIATION_2026-11-24.md) | Security findings and remediation evidence |

## Architecture and infrastructure

- [Azure IaC README](../infra/azure/README.md) — Bicep scope, deployment guardrails and verified subscription preparation.
- [Kafka production HA requirements](KAFKA_PRODUCTION_HA_REQUIREMENTS.md) — production requirements for a Kafka-based design.
- [Local infrastructure readiness audit](../LOCAL_INFRASTRUCTURE_READINESS_AUDIT_2026-11-24.md) — local stack assessment.
- [Docker Engine recovery and runtime check](DOCKER_ENGINE_RECOVERY_AND_RUNTIME_CHECK_2026-11-24.md) — Docker/WSL operational findings.
- [Runtime resource and MQTT bridge tuning](RUNTIME_RESOURCE_AND_MQTT_BRIDGE_TUNING_2026-11-24.md) — host-resource and bridge tuning evidence.

## IoT, Wokwi and lineage

- [Wokwi integration](WOKWI_INTEGRATION_2026-11-25.md) — integration design and setup.
- [Wokwi runtime lineage](WOKWI_RUNTIME_LINEAGE_2026-11-25.md) — runtime evidence.
- [Wokwi exact-record lineage](WOKWI_EXACT_RECORD_LINEAGE_2026-11-25.md) — record-level traceability evidence.
- [Wokwi acceptance follow-up](WOKWI_ACCEPTANCE_FOLLOWUP_2026-11-25.md) — acceptance status and remaining checks.
- [IoT simulator design and test](IOT_SIMULATOR_DESIGN_AND_TEST_2026-11-24.md) — simulator design and test notes.
- [Optional IoT source control](OPTIONAL_IOT_SOURCE_CONTROL_2026-11-25.md) — dashboard/control API behavior.

## Reliability, backup and acceptance

- [Pipeline soak acceptance](PIPELINE_SOAK_ACCEPTANCE_2026-11-24.md) — soak-test scope and results.
- [R2 backup setup](R2_BACKUP_SETUP.md) — object-storage backup configuration.
- [R2 restore drill report](../backups/2026-11-24/R2_RESTORE_DRILL_REPORT.md) — restore-drill evidence (local backup archive).
- [Follow-up execution](FOLLOW_UP_EXECUTION_2026-11-24.md) — dated follow-up actions.
- [Git history cleanup](GIT_HISTORY_CLEANUP_2026-11-24.md) — repository secret-scrubbing evidence.

## Document maintenance

- Keep current decisions in the relevant architecture or readiness document.
- Keep procedures in runbooks; keep dated test outputs as immutable evidence snapshots.
- Update the readiness review when a gate changes, and record evidence and scope.
- Never replace a failed, partial or not-run result with an assumed pass.
