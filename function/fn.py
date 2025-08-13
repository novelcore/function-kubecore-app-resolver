"""Crossplane composition function: Kubecore App resolver.

Behavior:
- Reads the observed composite (XApp/App) from the request context.
- Fetches only referenced resources from the cluster (read-only):
  - XKubEnv by claim labels.
  - XGitHubProject by claim labels.
- Produces a resolved context object used by go-templating to render resources.
- Never creates or updates cluster objects.

Notes:
- Environment de-duplication is first-wins: the first entry for a given
  `kubenvRef.name` is kept and subsequent duplicates are ignored.
"""

from __future__ import annotations

import contextlib
import importlib
import grpc
from typing import Any, Dict, List, Optional, Tuple

from crossplane.function import logging, resource, response
from crossplane.function.proto.v1 import run_function_pb2 as fnv1
from crossplane.function.proto.v1 import run_function_pb2_grpc as grpcv1

# Best-effort import of Kubernetes client/config at module import time to satisfy
# linter preferences for top-level imports. Fallback to dynamic import in
# _KubeLister.__init__ if unavailable in the current environment (e.g., tests).
try:  # pragma: no cover - availability depends on execution environment
    from kubernetes import client as kube_client, config as kube_config  # type: ignore
except Exception:  # pragma: no cover - handled in _KubeLister
    kube_client = None  # type: ignore[assignment]
    kube_config = None  # type: ignore[assignment]


def _get(dct: Dict[str, Any] | None, path: List[str], default: Any = None) -> Any:
    cur: Any = dct or {}
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


class _KubeLister:
    """Thin wrapper around Kubernetes CustomObjectsApi for read-only list ops."""

    def __init__(self, timeout_seconds: int = 2):
        self.timeout_seconds = timeout_seconds
        # If top-level imports were unavailable, import dynamically without
        # using inline import statements to satisfy lint rules.
        global kube_client, kube_config
        if kube_client is None or kube_config is None:
            k8s = importlib.import_module("kubernetes")  # type: ignore[import-not-found]
            kube_client = getattr(k8s, "client")  # type: ignore[assignment]
            kube_config = getattr(k8s, "config")  # type: ignore[assignment]

        # Try in-cluster first, fall back to local kubeconfig for development.
        with contextlib.suppress(Exception):
            kube_config.load_incluster_config()
        with contextlib.suppress(Exception):
            kube_config.load_kube_config()

        self._api = kube_client.CustomObjectsApi()  # type: ignore

    def list_xkubenenvs_by_claim(
        self, name: str, namespace: Optional[str]
    ) -> List[Dict[str, Any]]:
        label_selector = f"crossplane.io/claim-name={name}"
        if namespace:
            label_selector += f",crossplane.io/claim-namespace={namespace}"
        # group: platform.kubecore.io, version: v1alpha1, plural: xkubenenvs
        objs = self._api.list_cluster_custom_object(  # type: ignore
            group="platform.kubecore.io",
            version="v1alpha1",
            plural="xkubenenvs",
            label_selector=label_selector,
            timeout_seconds=self.timeout_seconds,
        )
        return objs.get("items", [])

    def list_xgithubprojects_by_claim(
        self, name: str, namespace: Optional[str]
    ) -> List[Dict[str, Any]]:
        label_selector = f"crossplane.io/claim-name={name}"
        if namespace:
            label_selector += f",crossplane.io/claim-namespace={namespace}"
        # group: github.platform.kubecore.io, version: v1alpha1, plural: xgithubprojects
        objs = self._api.list_cluster_custom_object(  # type: ignore
            group="github.platform.kubecore.io",
            version="v1alpha1",
            plural="xgithubprojects",
            label_selector=label_selector,
            timeout_seconds=self.timeout_seconds,
        )
        return objs.get("items", [])


def _summarize_kubenv(k: Dict[str, Any]) -> Dict[str, Any]:
    spec = k.get("spec", {}) if isinstance(k, dict) else {}
    meta = k.get("metadata", {}) if isinstance(k, dict) else {}
    labels = meta.get("labels", {}) if isinstance(meta, dict) else {}
    return {
        "found": True,
        "resourceName": meta.get("name"),
        "claimName": labels.get("crossplane.io/claim-name"),
        "spec": {
            "environmentType": spec.get("environmentType"),
            "resources": spec.get("resources", {}),
            "environmentConfig": spec.get("environmentConfig", {}),
            "qualityGates": spec.get("qualityGates", []),
            "sdlc": spec.get("sdlc"),
            "kubeClusterRef": spec.get("kubeClusterRef"),
        },
    }


def _resolve_project(
    lister: _KubeLister, project_name: Optional[str], project_namespace: Optional[str]
) -> Tuple[Dict[str, Any], Optional[str]]:
    """Resolve XGitHubProject by claim labels.

    Returns a tuple of (project dict, warning string or None).
    """
    project: Dict[str, Any] = {
        "name": project_name,
        "namespace": project_namespace,
        "providerConfigs": {},
    }
    if not project_name:
        return project, None
    try:
        items = lister.list_xgithubprojects_by_claim(project_name, project_namespace)
    except Exception as exc:  # Defensive: surface as warning, non-fatal
        return project, f"failed to list XGitHubProject for claim {project_name}: {exc}"

    if items:
        first = items[0]
        meta = first.get("metadata", {})
        status = first.get("status", {})
        provider_cfg = (
            status.get("providerConfig", {}) if isinstance(status, dict) else {}
        )
        project.update(
            {
                "resourceName": meta.get("name"),
                "providerConfigs": {
                    k: v for k, v in provider_cfg.items() if isinstance(k, str)
                },
                "status": (
                    status.get("conditions", status)
                    if isinstance(status, dict)
                    else status
                ),
            }
        )
        return project, None

    return project, f"XGitHubProject not found for claim {project_name}"


class FunctionRunner(grpcv1.FunctionRunnerService):
    """A FunctionRunner handles gRPC RunFunctionRequests."""

    def __init__(self, lister: Optional[_KubeLister] = None):
        """Create a new FunctionRunner."""
        self.log = logging.get_logger()
        self._lister = lister or _KubeLister()

    async def RunFunction(  # noqa: PLR0915 - function is intentionally linear for clarity
        self, req: fnv1.RunFunctionRequest, _: grpc.aio.ServicerContext
    ) -> fnv1.RunFunctionResponse:
        """Run the function."""
        # Some proto fields may be unset during render; access defensively.
        try:  # pragma: no cover - simple defensive access
            tag = req.meta.tag
        except Exception:
            tag = ""
        log = self.log.bind(tag=tag)
        log.info("Running function")

        # Build a response based on the request using SDK helper.
        rsp = response.to(req)

        xr = resource.struct_to_dict(req.observed.composite.resource)

        # Extract app spec
        app_name = (
            _get(xr, ["spec", "claimRef", "name"]) or _get(xr, ["metadata", "name"]) or ""
        )
        app_obj = {
            "name": app_name,
            "type": _get(xr, ["spec", "type"]),
            "image": _get(xr, ["spec", "image"]),
            "port": _get(xr, ["spec", "port"]),
        }

        project_ref = _get(xr, ["spec", "githubProjectRef"], {}) or {}
        project_name = project_ref.get("name")
        project_namespace = project_ref.get("namespace")

        # Resolve project
        project_obj, project_warning = _resolve_project(
            self._lister, project_name, project_namespace
        )
        if project_warning:
            response.warning(rsp, project_warning)

        # Environments - de-duplicate by kubenvRef.name (first-wins)
        env_specs = _get(xr, ["spec", "environments"], []) or []
        seen_env_names: set[str] = set()

        referenced_names: List[str] = []
        found_names: List[str] = []
        env_resolved: List[Dict[str, Any]] = []

        for env in env_specs:
            kubenv_ref = (env or {}).get("kubenvRef", {}) or {}
            env_name = kubenv_ref.get("name")
            if not env_name:
                # Skip entries without a name; surface as warning but non-fatal
                response.warning(
                    rsp,
                    (
                        "environment entry without kubenvRef.name encountered; "
                        "skipping"
                    ),
                )
                continue
            if env_name in seen_env_names:
                # First-wins policy
                continue
            seen_env_names.add(env_name)

            env_ns = kubenv_ref.get("namespace", "default")
            referenced_names.append(env_name)

            # Look up XKubEnv by claim labels
            kubenv_block: Dict[str, Any]
            try:
                items = self._lister.list_xkubenenvs_by_claim(env_name, env_ns)
            except Exception as exc:
                msg = f"failed to list XKubEnv for claim {env_name}: {exc}"
                response.warning(rsp, msg)
                items = []

            if items:
                found_names.append(env_name)
                kubenv_block = _summarize_kubenv(items[0])
            else:
                kubenv_block = {"found": False}

            env_resolved.append(
                {
                    "name": env_name,
                    "namespace": env_ns,
                    "enabled": bool((env or {}).get("enabled", False)),
                    "overrides": (env or {}).get("overrides", {}) or {},
                    "kubenv": kubenv_block,
                }
            )

        missing_names = sorted(set(referenced_names) - set(found_names))

        app_resolved = {
            "app": app_obj,
            "project": project_obj,
            "environments": env_resolved,
            "summary": {
                "referencedKubenvNames": referenced_names,
                "foundKubenvNames": found_names,
                "missingKubenvNames": missing_names,
                "counts": {
                    "referenced": len(set(referenced_names)),
                    "found": len(set(found_names)),
                    "missing": len(set(missing_names)),
                },
            },
        }

        # Write into namespaced context key
        ctx_key = "apiextensions.crossplane.io/context.kubecore.io"
        current_ctx = resource.struct_to_dict(rsp.context)
        current_ctx[ctx_key] = {"appResolved": app_resolved}
        rsp.context = resource.dict_to_struct(current_ctx)

        response.normal(rsp, "function-kubecore-app-resolver completed")
        log.info("Resolve app context complete")
        return rsp
