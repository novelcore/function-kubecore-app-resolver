# Changelog

## v0.0.0-next

- Fix: Discover KubEnv claims (namespaced) and match by `namespace/name` from `XApp.spec.environments[*].kubenvRef`.
- Feature: Enrich context under `apiextensions.crossplane.io/context.kubecore.io` with:
  - `appResolved.summary` counters and namespaced referenced/found/missing sets
  - `appResolved.environments[*].kubenv` with `found`, `resourceName`, `spec`, `labels`, `annotations`, `claimName`
  - `kubenvLookup` map with both `name` and `namespace/name` keys
  - `allKubenvs` array of sanitized KubEnv claims
  - Back-compat helpers: `$resolved`, `$resolvedEnvs`, `$summary`
- Observability: Add structured JSON logging for each step with `event`, `step`, `tag`, counts, and timing.
- RBAC: Include read-only access for `kubenvs` (claims) in `package/crossplane.yaml`.

