# function-kubecore-app-resolver

[![CI](https://github.com/novelcore/function-kubecore-app-resolver/actions/workflows/ci.yml/badge.svg)](https://github.com/novelcore/function-kubecore-app-resolver/actions/workflows/ci.yml)

A Crossplane composition function (Python) that will resolve Kubecore App context.

Development workflow:

```shell
# Run the function in development mode (used by crossplane render)
hatch run development

# Lint and format the code - see pyproject.toml
hatch fmt

# Run unit tests - see tests/test_fn.py
hatch test

# Build the function's runtime image - see Dockerfile
docker build . --tag=runtime

# Build a function package - see package/crossplane.yaml
crossplane xpkg build -f package --embed-runtime-image=runtime
```



## Logging

This function emits structured JSON logs with high-signal events for observability.

Examples (keys may include a `tag` when provided by the Function runtime):

```json
{"level":"info","event":"Start","step":"resolve-app-context","xr":{"name":"<xr>","kind":"XApp","apiVersion":"app.kubecore.io/v1alpha1"},"tag":"..."}
{"level":"debug","event":"Input environments","count":2,"items":[{"name":"demo-dev","namespace":"test","enabled":true}],"tag":"..."}
{"level":"info","event":"Listing KubEnv claims","namespaces":["test"],"mode":"namespaced","tag":"..."}
{"level":"debug","event":"KubEnv claims found","total":2,"sample":[{"ns":"test","name":"demo-dev"}],"tag":"..."}
{"level":"debug","event":"Matched environments","foundCount":2,"found":["test/demo-dev"],"missing":[],"tag":"..."}
{"level":"info","event":"Context populated","summary":{"referenced":["test/demo-dev"],"found":["test/demo-dev"],"missing":[]},"durationMs":7,"tag":"..."}
{"level":"info","event":"Complete","step":"resolve-app-context","durationMs":7,"tag":"..."}
```

Log levels:

- INFO: start/completion of major steps
- DEBUG: detailed lists and matching results
- WARN: missing resources or degraded behavior
- ERROR: API failures (includes namespace/status)

## RBAC
The function requires read-only access to `xkubenenvs`, `kubenvs`, and `xgithubprojects`. The package `package/crossplane.yaml` includes the minimal rules.

## Testing locally
Use `crossplane render` with the example `composition.yaml` and your XR to inspect the function output. The resolved context is available under `apiextensions.crossplane.io/context.kubecore.io`.

## Release and image
Build and push an image:

```bash
docker build . -t ghcr.io/novelcore/function-kubecore-app-resolver:v0.0.0-$(date +%Y%m%d)-$(git rev-parse --short HEAD)
docker push ghcr.io/novelcore/function-kubecore-app-resolver:v0.0.0-$(date +%Y%m%d)-$(git rev-parse --short HEAD)
```

Update your Function package to reference the new image tag or run with the Development runtime during testing.
