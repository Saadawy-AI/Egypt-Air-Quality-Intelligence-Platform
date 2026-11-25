# Azure Infrastructure

This directory contains the initial Azure Infrastructure as Code foundation. It is a compatibility-spike starting point, **not a production deployment package**.

## Architecture direction

The current candidate target is:

- Azure Event Hubs with Kafka endpoint (Premium is the preferred target, subject to compatibility, quota and cost validation).
- ADLS Gen2 with hierarchical namespace.
- Azure Database for PostgreSQL Flexible Server.
- Entra workload identities and least-privilege RBAC.
- Private networking/private DNS and Azure Monitor.
- Key Vault only for secrets that cannot be replaced by managed identity.

The architecture decision and trade-offs are documented in [the streaming architecture decision](../../docs/architecture/AZURE_STREAMING_ARCHITECTURE_DECISION_2026-11-24.md).

## Files

| File | Purpose |
|---|---|
| `main.bicep` | Initial optional Storage, Event Hubs Standard and Log Analytics resources |
| `main.parameters.example.json` | Non-secret parameter example; deployment flags are disabled |
| `../../.github/workflows/ci.yml` | CI validation and Bicep compilation |

## Verified preparation state (2026-11-25)

- Azure CLI login succeeded for the enabled default subscription.
- Selected region: **UAE North**.
- Subscription role assignment query returned **Owner**.
- `Microsoft.Storage`, `Microsoft.EventHub` and `Microsoft.DBforPostgreSQL` resource providers are registered.
- GitHub Actions CI run #6 passed Python regression, Compose validation and Bicep compilation.
- The owner reported $190 promotional credit, authorized up to $185 of usage, and requested an alert when remaining credit reaches $20. Credit expiry and billing details have not yet been independently verified.
- Azure Budgets query returned an empty list; no budget was visible through that query.
- **No Azure resources have been deployed.**

## Template limitations

The current Bicep foundation is intentionally incomplete. It does not yet provision the complete production topology, including:

- Virtual network, delegated PostgreSQL subnet, private endpoints and private DNS.
- PostgreSQL Flexible Server and its backup/HA configuration.
- Managed identities and scoped role assignments.
- Key Vault policies/RBAC and secret rotation controls.
- Complete Event Hubs entities, diagnostics, alerts and operational dashboards.
- Container registry, compute/orchestration, CI/CD deployment identity and supply-chain controls.
- Tested disaster recovery, fault injection, replay and end-to-end cloud lineage.

The current template uses a small Event Hubs Standard baseline and has public-network exposure enabled for the initial compatibility scope. Do not interpret successful Bicep compilation as secure or production-ready infrastructure.

## Deployment guardrails

1. Do not run `az deployment`, create resources or enable paid services without explicit owner approval after reviewing the deployment plan.
2. Verify credit type, remaining balance, expiry and billing scope in Cost Management + Billing.
3. Configure budget notifications and validate recipients. Budgets notify; they do not automatically stop resources.
4. The requested $20 remaining-credit warning and $185 maximum authorized usage are operational guardrails, not a hard Azure spending cap. Define a manual/automated shutdown procedure separately and account for billing/credit reporting delay.
5. Confirm regional service availability, quotas, SKU constraints and a written cost estimate in UAE North.
6. Add private networking, identity and diagnostics before any workload is exposed.
7. Run Bicep build and deployment what-if with reviewed parameters before deployment.
8. Preserve environment isolation and use unique cloud storage/checkpoint paths; never reuse local Docker volumes as cloud state.
9. Never commit subscription identifiers, tenant identifiers, credentials, SAS tokens, connection strings or private keys.

## Validation

From the repository root:

```bash
az bicep build --file infra/azure/main.bicep --stdout > /dev/null
```

A successful compile checks template syntax only. It does not validate quota, policy, service availability, cost, permissions for every resource, runtime compatibility or production security.

## Readiness decision

Current status remains **NOT READY FOR AZURE DEPLOYMENT**. Continue with read-only account, billing, quota and service-availability checks, then close the architecture/security/IaC gaps before considering a non-production deployment. See [the Azure readiness review](../../docs/reports/AZURE_READINESS_REVIEW_2026-11-25.md).
