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

[functions]: https://docs.crossplane.io/latest/concepts/composition-functions
[function guide]: https://docs.crossplane.io/knowledge-base/guides/write-a-composition-function-in-python
[package docs]: https://crossplane.github.io/function-sdk-python
[python]: https://python.org
[docker]: https://www.docker.com
[cli]: https://docs.crossplane.io/latest/cli
