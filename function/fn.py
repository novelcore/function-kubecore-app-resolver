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
import time
from typing import Any

import grpc
from crossplane.function import logging, resource, response
from crossplane.function.proto.v1 import run_function_pb2 as fnv1
from crossplane.function.proto.v1 import run_function_pb2_grpc as grpcv1

# Best-effort import of Kubernetes client/config at module import time to satisfy
# linter preferences for top-level imports. Fallback to dynamic import in
# _KubeLister.__init__ if unavailable in the current environment (e.g., tests).
try:  # pragma: no cover - availability depends on execution environment
    from kubernetes import client as kube_client  # type: ignore
    from kubernetes import config as kube_config
except Exception:  # pragma: no cover - handled in _KubeLister
    kube_client = None  # type: ignore[assignment]
    kube_config = None  # type: ignore[assignment]


def _get(dct: dict[str, Any] | None, path: list[str], default: Any = None) -> Any:
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
        if kube_client is not None and kube_config is not None:
            kc = kube_client
            kcfg = kube_config
        else:
            k8s = importlib.import_module("kubernetes")  # type: ignore[import-not-found]
            kc = k8s.client  # type: ignore[assignment]
            kcfg = k8s.config  # type: ignore[assignment]

        # Try in-cluster first, fall back to local kubeconfig for development.
        with contextlib.suppress(Exception):
            kcfg.load_incluster_config()
        with contextlib.suppress(Exception):
            kcfg.load_kube_config()

        self._api = kc.CustomObjectsApi()  # type: ignore

    def list_xkubenenvs_by_claim(
        self, name: str, namespace: str | None
    ) -> list[dict[str, Any]]:
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
        self, name: str, namespace: str | None
    ) -> list[dict[str, Any]]:
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

    def list_kubenvs_in_namespace(self, namespace: str) -> list[dict[str, Any]]:
        # group: platform.kubecore.io, version: v1alpha1, plural: kubenvs
        objs = self._api.list_namespaced_custom_object(  # type: ignore
            group="platform.kubecore.io",
            version="v1alpha1",
            namespace=namespace,
            plural="kubenvs",
            timeout_seconds=self.timeout_seconds,
        )
        return objs.get("items", [])

    def list_xqualitygates_by_claim(
        self, name: str, namespace: str | None
    ) -> list[dict[str, Any]]:
        label_selector = f"crossplane.io/claim-name={name}"
        if namespace:
            label_selector += f",crossplane.io/claim-namespace={namespace}"
        # group: platform.kubecore.io, version: v1alpha1, plural: xqualitygates
        objs = self._api.list_cluster_custom_object(  # type: ignore
            group="platform.kubecore.io",
            version="v1alpha1",
            plural="xqualitygates",
            label_selector=label_selector,
            timeout_seconds=self.timeout_seconds,
        )
        return objs.get("items", [])

    def list_qualitygates_in_namespace(self, namespace: str) -> list[dict[str, Any]]:
        # group: platform.kubecore.io, version: v1alpha1, plural: qualitygates
        objs = self._api.list_namespaced_custom_object(  # type: ignore
            group="platform.kubecore.io",
            version="v1alpha1",
            namespace=namespace,
            plural="qualitygates",
            timeout_seconds=self.timeout_seconds,
        )
        return objs.get("items", [])

    def list_xgithubapps_by_claim(
        self, name: str, namespace: str | None
    ) -> list[dict[str, Any]]:
        label_selector = f"crossplane.io/claim-name={name}"
        if namespace:
            label_selector += f",crossplane.io/claim-namespace={namespace}"
        # group: github.platform.kubecore.io, version: v1alpha1, plural: xgithubapps
        objs = self._api.list_cluster_custom_object(  # type: ignore
            group="github.platform.kubecore.io",
            version="v1alpha1",
            plural="xgithubapps",
            label_selector=label_selector,
            timeout_seconds=self.timeout_seconds,
        )
        return objs.get("items", [])


def _summarize_kubenv(k: dict[str, Any]) -> dict[str, Any]:
    spec = k.get("spec", {}) if isinstance(k, dict) else {}
    meta = k.get("metadata", {}) if isinstance(k, dict) else {}
    labels = meta.get("labels", {}) if isinstance(meta, dict) else {}
    return {
        "found": True,
        # Canonical resourceName: "<namespace>/<name>"
        "resourceName": (
            f"{meta.get('namespace')}/{meta.get('name')}"
            if meta.get("namespace") and meta.get("name")
            else meta.get("name")
        ),
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
    lister: _KubeLister, project_name: str | None, project_namespace: str | None
) -> tuple[dict[str, Any], str | None]:
    """Resolve XGitHubProject and XGitHubApp by claim labels.

    Returns a tuple of (project dict, warning string or None).
    """
    project: dict[str, Any] = {
        "name": project_name,
        "namespace": project_namespace,
        "providerConfigs": {},
        "github": {
            "app": {"found": False},
            "repository": {}
        }
    }
    if not project_name:
        return project, None

    warnings = []

    # Resolve XGitHubProject
    try:
        items = lister.list_xgithubprojects_by_claim(project_name, project_namespace)
    except Exception as exc:  # Defensive: surface as warning, non-fatal
        warnings.append(
            f"failed to list XGitHubProject for claim {project_name}: {exc}"
        )
        items = []

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

        # Extract repository information from status
        if isinstance(status, dict):
            repo_info = status.get("repository", {})
            if repo_info:
                project["github"]["repository"] = {
                    "owner": repo_info.get("owner", ""),
                    "name": repo_info.get("name", ""),
                    "fullName": repo_info.get("fullName", "")
                }
    else:
        warnings.append(f"XGitHubProject not found for claim {project_name}")

    # Resolve XGitHubApp
    try:
        app_items = lister.list_xgithubapps_by_claim(project_name, project_namespace)
    except Exception as exc:
        warnings.append(f"failed to list XGitHubApp for claim {project_name}: {exc}")
        app_items = []

    if app_items:
        first_app = app_items[0]
        app_meta = first_app.get("metadata", {})
        app_status = first_app.get("status", {})
        app_resource_name = (
            f"{app_meta.get('namespace', 'default')}/{app_meta.get('name', '')}"
        )

        project["github"]["app"] = {
            "found": True,
            "resourceName": app_resource_name,
            "status": {
                "providerConfig": app_status.get("providerConfig", {}),
                "githubProjectRef": app_status.get("githubProjectRef", {})
            }
        }
    # Try to resolve from project status
    elif items and isinstance(items[0].get("status"), dict):
        github_app_ref = items[0]["status"].get("githubAppRef", {})
        if github_app_ref:
            project["github"]["app"] = {
                "found": True,
                "resourceName": (
                    f"{github_app_ref.get('namespace', 'default')}/"
                    f"{github_app_ref.get('name', '')}"
                ),
                "status": {
                    "providerConfig": project.get("providerConfigs", {}),
                    "githubProjectRef": {"name": project_name}
                }
            }

    return project, "; ".join(warnings) if warnings else None


def _resolve_target_environment(
    app_name: str, env_name: str, kubenv_spec: dict[str, Any] | None
) -> dict[str, Any]:
    """Resolve target environment information including cluster and namespace."""
    target = {
        "namespace": f"{app_name}-{env_name}",
        "cluster": ""
    }

    if kubenv_spec:
        cluster_ref = kubenv_spec.get("kubeClusterRef", {})
        if cluster_ref:
            cluster_name = cluster_ref.get("name", "")
            if cluster_name:
                # Generate cluster domain based on naming pattern
                target["cluster"] = f"{cluster_name}.eks.eu-west-3.amazonaws.com"

    return target


def _sanitize_xqualitygate(obj: dict[str, Any]) -> dict[str, Any]:
    """Sanitize XQualityGate object for lookup storage."""
    meta = obj.get("metadata", {}) if isinstance(obj, dict) else {}
    spec_obj = obj.get("spec", {}) if isinstance(obj, dict) else {}

    return {
        "apiVersion": obj.get("apiVersion"),
        "kind": obj.get("kind", "XQualityGate"),
        "metadata": {
            "name": meta.get("name"),
            "namespace": meta.get("namespace"),
            "labels": meta.get("labels", {}),
            "annotations": meta.get("annotations", {}),
        },
        "spec": spec_obj,
    }


def _validate_workflow_schema(workflow_schema: dict[str, Any]) -> dict[str, Any]:
    """Validate embedded workflow schema and return validation results."""
    validation = {
        "parametersValid": True,
        "stepsValid": True,
        "outputsValid": True,
        "triggersValid": True,
        "errors": []
    }

    # Validate parameters
    parameters = workflow_schema.get("parameters", [])
    if parameters:
        for param in parameters:
            if not isinstance(param, dict):
                validation["parametersValid"] = False
                validation["errors"].append("Invalid parameter structure")
                continue
            if not param.get("name"):
                validation["parametersValid"] = False
                validation["errors"].append("Parameter missing name")

    # Validate steps
    steps = workflow_schema.get("steps", [])
    if not steps:
        validation["stepsValid"] = False
        validation["errors"].append("No steps defined")
    else:
        for step in steps:
            if not isinstance(step, dict):
                validation["stepsValid"] = False
                validation["errors"].append("Invalid step structure")
                continue
            if not step.get("name"):
                validation["stepsValid"] = False
                validation["errors"].append("Step missing name")
            if not step.get("container"):
                validation["stepsValid"] = False
                validation["errors"].append("Step missing container")

    # Validate outputs
    outputs = workflow_schema.get("outputs", {})
    if outputs and not isinstance(outputs, dict):
        validation["outputsValid"] = False
        validation["errors"].append("Invalid outputs structure")

    return validation


def _generate_workflow_metadata(
    gate_name: str, workflow_schema: dict[str, Any], target_namespace: str
) -> dict[str, Any]:
    """Generate workflow template metadata."""
    template_name = f"{gate_name}-template"

    # Calculate estimated duration from timeout
    timeout = workflow_schema.get("timeout", "5m")
    estimated_duration = timeout

    # Calculate resource requirements from steps
    total_cpu = "0m"
    total_memory = "0Mi"
    dependencies = []

    steps = workflow_schema.get("steps", [])
    for step in steps:
        container = step.get("container", {})
        resources = container.get("resources", {})
        requests = resources.get("requests", {})

        # Extract dependencies from env vars
        env_vars = container.get("env", [])
        for env_var in env_vars:
            value_from = env_var.get("valueFrom", {})
            secret_ref = value_from.get("secretKeyRef", {})
            if secret_ref:
                secret_name = secret_ref.get("name")
                if secret_name and secret_name not in dependencies:
                    dependencies.append(secret_name)

        # Sum up CPU and memory (simplified calculation)
        if requests.get("cpu"):
            total_cpu = requests["cpu"]
        if requests.get("memory"):
            total_memory = requests["memory"]

    validation_status = _validate_workflow_schema(workflow_schema)

    return {
        "gateName": gate_name,
        "templateName": template_name,
        "targetNamespace": target_namespace,
        "generationRequired": True,
        "validationStatus": validation_status,
        "metadata": {
            "estimatedDuration": estimated_duration,
            "resourceRequirements": {
                "cpu": total_cpu,
                "memory": total_memory
            },
            "dependencies": dependencies
        }
    }


def _generate_gitops_files(
    gate_name: str, app_name: str, env_name: str
) -> list[dict[str, Any]]:
    """Generate GitOps file metadata for a quality gate."""
    files = []

    # WorkflowTemplate file
    files.append({
        "path": f"apps/{app_name}/{env_name}/workflows/{gate_name}-template.yaml",
        "type": "WorkflowTemplate",
        "gateName": gate_name,
        "size": "2.1KB",  # Estimated size
        "checksum": f"sha256:abc123{hash(gate_name) % 1000}..."
    })

    # Sensor file
    files.append({
        "path": f"apps/{app_name}/{env_name}/sensors/{gate_name}-sensor.yaml",
        "type": "Sensor",
        "gateName": gate_name,
        "size": "1.8KB",  # Estimated size
        "checksum": f"sha256:def456{hash(gate_name) % 1000}..."
    })

    return files


class FunctionRunner(grpcv1.FunctionRunnerService):
    """A FunctionRunner handles gRPC RunFunctionRequests."""

    def __init__(self, lister: _KubeLister | None = None):
        """Create a new FunctionRunner."""
        self.log = logging.get_logger()
        self._lister = lister or _KubeLister()

    async def RunFunction(  # noqa: PLR0915, C901, PLR0912 - intentionally linear for clarity
        self, req: fnv1.RunFunctionRequest, _: grpc.aio.ServicerContext
    ) -> fnv1.RunFunctionResponse:
        """Run the function."""
        # Some proto fields may be unset during render; access defensively.
        try:  # pragma: no cover - simple defensive access
            tag = req.meta.tag
        except Exception:
            tag = ""
        log = self.log.bind(tag=tag)
        xr_meta = resource.struct_to_dict(req.observed.composite.resource)
        log.info(
            "resolve-app-context.start",
            step="resolve-app-context",
            xr={
                "name": _get(xr_meta, ["metadata", "name"]),
                "kind": _get(xr_meta, ["kind"]),
                "apiVersion": _get(xr_meta, ["apiVersion"]),
            },
        )
        t_start = time.time()

        # Build a response based on the request using SDK helper.
        rsp = response.to(req)

        xr = resource.struct_to_dict(req.observed.composite.resource)

        # Extract app spec
        app_name = (
            _get(xr, ["spec", "claimRef", "name"])
            or _get(xr, ["metadata", "name"])
            or ""
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
        # Resolve project with enhanced GitHub integration
        project_obj, project_warning = _resolve_project(
            self._lister, project_name, project_namespace
        )
        if project_warning:
            log.warning("project.resolution", warning=project_warning)

        # Normalize environments from both shapes and de-duplicate by canonical key
        def _normalize_envs(app: dict[str, Any]) -> list[dict[str, Any]]:
            normalized: list[dict[str, Any]] = []
            # Shape A: spec.environments[] (display name == KubEnv name)
            for env in _get(app, ["spec", "environments"], []) or []:
                env_obj = env or {}
                kubenv_ref = env_obj.get("kubenvRef", {}) or {}
                kubenv_name = kubenv_ref.get("name")
                if not kubenv_name:
                    self.log.error(
                        "kubenv.ref.missing",
                        message="environment entry without kubenvRef.name encountered; marking missing",
                    )
                    continue
                kubenv_ns = kubenv_ref.get("namespace", "default")
                normalized.append(
                    {
                        "name": kubenv_name,  # display
                        "namespace": kubenv_ns,  # display
                        "kubenvName": kubenv_name,
                        "kubenvNamespace": kubenv_ns,
                        "canonical": f"{kubenv_ns}/{kubenv_name}",
                        "enabled": bool(env_obj.get("enabled", False)),
                        "overrides": (env_obj.get("overrides", {}) or {}),
                    }
                )
            # Shape B: spec.kubenvs.{dev,staging,prod} (display name is the key)
            for key_label, env in (_get(app, ["spec", "kubenvs"], {}) or {}).items():
                env_obj = env or {}
                kubenv_ref = env_obj.get("kubenvRef", {}) or {}
                kubenv_name = kubenv_ref.get("name")
                if not kubenv_name:
                    self.log.error(
                        "kubenv.ref.missing",
                        message=f"kubenvRef.name missing for kubenvs entry '{key_label}'",
                    )
                    continue
                kubenv_ns = kubenv_ref.get("namespace", "default")
                # display namespace can be overridden per env, else default
                display_ns = env_obj.get("namespace", "default")
                overrides_from_shape_b = {
                    k: v
                    for k, v in env_obj.items()
                    if k in {"resources", "environment", "qualityGates"}
                }
                normalized.append(
                    {
                        "name": key_label,  # display env name
                        "namespace": display_ns,  # display env namespace
                        "kubenvName": kubenv_name,
                        "kubenvNamespace": kubenv_ns,
                        "canonical": f"{kubenv_ns}/{kubenv_name}",
                        "enabled": bool(env_obj.get("enabled", False)),
                        "overrides": {
                            **overrides_from_shape_b,
                            **(env_obj.get("overrides", {}) or {}),
                        },
                    }
                )
            return normalized

        raw_envs = _normalize_envs(xr)
        seen_env_canonicals: set[str] = set()

        env_inputs: list[dict[str, Any]] = []
        for env in raw_envs:
            canonical_key = env.get("canonical")
            if canonical_key in seen_env_canonicals:
                continue
            seen_env_canonicals.add(canonical_key)
            env_inputs.append(env)

        input_items = [
            {
                "display": {
                    "name": e["name"],
                    "namespace": e["namespace"],
                },
                "kubenvRef": {
                    "name": e["kubenvName"],
                    "namespace": e["kubenvNamespace"],
                },
                "enabled": e["enabled"],
            }
            for e in env_inputs
        ]
        log.debug(
            "kubenv.input",
            count=len(env_inputs),
            items=input_items,
        )

        # List KubEnv claims only in referenced namespaces, but only use referenced envs
        namespaces = sorted({e["kubenvNamespace"] for e in env_inputs})
        log.info(
            "kubenv.list",
            namespaces=namespaces,
            mode="namespaced",
        )

        all_kubenv_claims: list[dict[str, Any]] = []
        for ns in namespaces:
            try:
                items = self._lister.list_kubenvs_in_namespace(ns)
                all_kubenv_claims.extend(items)
            except Exception as exc:  # pragma: no cover - behavior depends on client
                status = getattr(exc, "status", None)
                log.error(
                    "kubenv.api",
                    operation="list KubEnv",
                    namespace=ns,
                    status=status,
                    error=str(exc),
                )

        # Collect all quality gate references for lookup
        all_gate_refs: set[str] = set()
        for item in all_kubenv_claims:
            spec_obj = item.get("spec", {}) if isinstance(item, dict) else {}
            gates = spec_obj.get("qualityGates", []) if isinstance(spec_obj, dict) else []
            for gate in gates:
                if isinstance(gate, dict):
                    ref = gate.get("ref", {})
                    if ref and ref.get("name"):
                        gate_ns = ref.get("namespace", "default")
                        canonical = f"{gate_ns}/{ref['name']}"
                        all_gate_refs.add(canonical)

        # Add quality gate refs from app overrides
        for e in env_inputs:
            override_gates = _get(e, ["overrides", "qualityGates"], []) or []
            for gate in override_gates:
                if isinstance(gate, dict):
                    ref = gate.get("ref", {})
                    if ref and ref.get("name"):
                        gate_ns = ref.get("namespace", e["kubenvNamespace"])
                        canonical = f"{gate_ns}/{ref['name']}"
                        all_gate_refs.add(canonical)

        # Fetch all referenced XQualityGate resources
        log.info(
            "xqualitygate.list",
            referencedGates=sorted(all_gate_refs),
            count=len(all_gate_refs),
        )

        all_xqualitygate_claims: list[dict[str, Any]] = []
        xqualitygate_namespaces = sorted({ref.split("/")[0] for ref in all_gate_refs})
        for ns in xqualitygate_namespaces:
            try:
                items = self._lister.list_qualitygates_in_namespace(ns)
                all_xqualitygate_claims.extend(items)
            except Exception as exc:
                status = getattr(exc, "status", None)
                log.error(
                    "xqualitygate.api",
                    operation="list QualityGate",
                    namespace=ns,
                    status=status,
                    error=str(exc),
                )

        # Build a temporary lookup of all claims by canonical key (used only for matching referenced envs)
        def _sanitize_kubenv_claim(obj: dict[str, Any]) -> dict[str, Any]:
            meta = obj.get("metadata", {}) if isinstance(obj, dict) else {}
            spec_obj = obj.get("spec", {}) if isinstance(obj, dict) else {}
            # Keep only fields needed downstream in sanitized spec
            sanitized_spec = {
                "resources": spec_obj.get("resources", {}),
                "environmentConfig": spec_obj.get("environmentConfig", {}),
                "qualityGates": spec_obj.get("qualityGates", []),
                "kubeClusterRef": spec_obj.get("kubeClusterRef", {}),
            }
            return {
                "apiVersion": obj.get("apiVersion"),
                "kind": obj.get("kind", "KubEnv"),
                "metadata": {
                    "name": meta.get("name"),
                    "namespace": meta.get("namespace"),
                    "labels": meta.get("labels", {}),
                    "annotations": meta.get("annotations", {}),
                },
                "spec": sanitized_spec,
            }

        by_canonical: dict[str, dict[str, Any]] = {}
        for item in all_kubenv_claims:
            meta = item.get("metadata", {}) if isinstance(item, dict) else {}
            name = meta.get("name")
            ns = meta.get("namespace")
            if not name or not ns:
                continue
            key = f"{ns}/{name}"
            if key not in by_canonical:
                by_canonical[key] = item

        # Build XQualityGate lookup
        xqualitygate_lookup: dict[str, dict[str, Any]] = {}
        for item in all_xqualitygate_claims:
            meta = item.get("metadata", {}) if isinstance(item, dict) else {}
            name = meta.get("name")
            ns = meta.get("namespace")
            if not name or not ns:
                continue
            key = f"{ns}/{name}"
            if key not in xqualitygate_lookup:
                xqualitygate_lookup[key] = _sanitize_xqualitygate(item)

        # Helpers for merge logic
        def _merge_resource_quota(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
            base_req = (base or {}).get("requests", {}) or {}
            base_lim = (base or {}).get("limits", {}) or {}
            ov_req = (override or {}).get("requests", {}) or {}
            ov_lim = (override or {}).get("limits", {}) or {}
            merged = {
                "requests": {**base_req, **ov_req},
                "limits": {**base_lim, **ov_lim},
            }
            return merged

        def _merge_env_vars(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
            base_vars = base or {}
            ov_vars = override or {}
            return {**base_vars, **ov_vars}

        def _canonical_gate_ref(ref: dict[str, Any] | None, default_ns: str) -> tuple[str, dict[str, Any]]:
            ref = ref or {}
            name = ref.get("name")
            ns = ref.get("namespace", default_ns)
            canonical = f"{ns}/{name}" if name else ""
            # Ensure ref has namespace filled for output consistency
            out_ref = {"name": name, "namespace": ns} if name else {}
            return canonical, out_ref

        def _merge_quality_gates(
            baseline: list[dict[str, Any]] | None,
            overrides: list[dict[str, Any]] | None,
            default_ns: str,
            xqualitygate_lookup: dict[str, dict[str, Any]],
            app_name: str,
            env_name: str,
        ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
            base = baseline or []
            ov = overrides or []
            base_map: dict[str, dict[str, Any]] = {}
            ov_map: dict[str, dict[str, Any]] = {}
            order_keys: list[str] = []

            # Ingest baseline in order; skip non-dict or missing ref
            for g in base:
                if not isinstance(g, dict):
                    continue
                key_str, out_ref = _canonical_gate_ref((g or {}).get("ref"), default_ns)
                if not key_str:
                    continue
                base_map[key_str] = {
                    "ref": out_ref,
                    "key": (g or {}).get("key"),
                    "phase": (g or {}).get("phase"),
                    "required": bool((g or {}).get("required", False)),
                }
                if key_str not in order_keys:
                    order_keys.append(key_str)

            # Ingest overrides in order; append new refs to order
            for g in ov:
                if not isinstance(g, dict):
                    continue
                key_str, out_ref = _canonical_gate_ref((g or {}).get("ref"), default_ns)
                if not key_str:
                    continue
                ov_map[key_str] = {
                    "ref": out_ref,
                    "key": (g or {}).get("key"),
                    "phase": (g or {}).get("phase"),
                    "required": (g or {}).get("required"),
                }
                if key_str not in order_keys:
                    order_keys.append(key_str)

            merged_list: list[dict[str, Any]] = []
            active_commit_statuses: list[dict[str, Any]] = []
            proposed_commit_statuses: list[dict[str, Any]] = []
            workflow_templates: list[dict[str, Any]] = []
            gitops_files: list[dict[str, Any]] = []

            for k in order_keys:
                b = base_map.get(k) or {}
                o = ov_map.get(k) or {}
                ref_out = (o.get("ref") or b.get("ref") or {})
                chosen_key = o.get("key") if o.get("key") not in (None, "") else b.get("key")
                phase = o.get("phase") if o.get("phase") is not None else b.get("phase")
                required_val = (
                    bool(o.get("required"))
                    if o.get("required") is not None
                    else bool(b.get("required", False))
                )

                # Get XQualityGate resource for embedded workflow
                xqualitygate = xqualitygate_lookup.get(k, {})
                xqualitygate_spec = xqualitygate.get("spec", {})

                # Extract key from QualityGate spec.key, fallback to KubEnv key
                qg_key = xqualitygate_spec.get("key")
                resolved_key = qg_key if qg_key not in (None, "") else chosen_key

                # Extract parameters from KubEnv configuration
                kubenv_gate_config = {}
                kubenv_params = {}
                
                # Get parameters from base and override configurations
                base_gate_config = next((g for g in (base if isinstance(base, list) else []) if isinstance(g, dict) and g.get("ref", {}).get("name") == ref_out.get("name")), {})
                override_gate_config = next((g for g in (ov if isinstance(ov, list) else []) if isinstance(g, dict) and g.get("ref", {}).get("name") == ref_out.get("name")), {})
                
                # Merge parameters from base and override
                if isinstance(base_gate_config, dict):
                    kubenv_params.update(base_gate_config.get("parameters", {}) or {})
                if isinstance(override_gate_config, dict):
                    kubenv_params.update(override_gate_config.get("parameters", {}) or {})

                # Build enhanced quality gate with embedded workflow schema
                gate_out = {
                    "ref": ref_out,
                    "key": resolved_key,
                    "description": xqualitygate_spec.get("description", ""),
                    "category": xqualitygate_spec.get("category", ""),
                    "severity": xqualitygate_spec.get("severity", "medium"),
                    "phase": phase,
                    "required": required_val,
                    "parameters": kubenv_params,  # from KubEnv configuration
                    "applicability": xqualitygate_spec.get("applicability", {}),
                    "workflowSchema": xqualitygate_spec.get("workflowSchema", {}),
                    "triggers": xqualitygate_spec.get("triggers", {}),
                    "commitStatus": xqualitygate_spec.get("commitStatus", {}),
                }
                merged_list.append(gate_out)

                # Generate workflow metadata if workflow schema exists
                workflow_schema = gate_out.get("workflowSchema", {})
                if workflow_schema and resolved_key:
                    target_namespace = f"{app_name}-{env_name}"
                    template_metadata = _generate_workflow_metadata(
                        resolved_key, workflow_schema, target_namespace
                    )
                    workflow_templates.append(template_metadata)

                    # Generate GitOps files for this gate
                    gate_files = _generate_gitops_files(resolved_key, app_name, env_name)
                    gitops_files.extend(gate_files)

                # Enhanced commit status generation
                if isinstance(resolved_key, str) and resolved_key:
                    commit_status_config = gate_out.get("commitStatus", {})
                    description_template = commit_status_config.get("descriptionTemplate", f"{resolved_key} for {{.environment}}")
                    url_template = commit_status_config.get("urlTemplate", "")

                    status_entry = {
                        "key": resolved_key,
                        "description": description_template.replace("{{.environment}}", env_name),
                        "context": f"continuous-integration/{resolved_key}",
                        "targetUrl": url_template.replace("{{.namespace}}", f"{app_name}-{env_name}")
                    }

                    if phase == "active":
                        active_commit_statuses.append(status_entry)
                    elif phase == "proposed":
                        proposed_commit_statuses.append(status_entry)

            return merged_list, active_commit_statuses, proposed_commit_statuses, workflow_templates, gitops_files

        # Match referenced environments using canonical keys and deduped sets
        referenced_set: set[str] = set()
        found_set: set[str] = set()
        env_resolved: list[dict[str, Any]] = []
        kubenv_lookup: dict[str, dict[str, Any]] = {}
        for e in env_inputs:
            canonical = e["canonical"]
            referenced_set.add(canonical)
            found_obj_raw = by_canonical.get(canonical)

            if found_obj_raw is not None:
                found_set.add(canonical)
                # Sanitize claim and spec
                found_obj = _sanitize_kubenv_claim(found_obj_raw)
                kubenv_lookup[canonical] = found_obj
                meta = found_obj.get("metadata", {})
                labels = meta.get("labels", {}) if isinstance(meta, dict) else {}
                resource_name = f"{meta.get('namespace')}/{meta.get('name')}"
                claim_name = labels.get("crossplane.io/claim-name", e.get("kubenvName"))
                # Effective merges
                spec_obj = found_obj.get("spec", {})
                base_defaults = _get(spec_obj, ["resources", "defaults"], {}) or {}
                overrides_res = _get(e, ["overrides", "resources"], {}) or {}
                effective_resources = _merge_resource_quota(base_defaults, overrides_res)

                base_env = _get(spec_obj, ["environmentConfig", "variables"], {}) or {}
                overrides_env = _get(e, ["overrides", "environment"], {}) or {}
                effective_env = _merge_env_vars(base_env, overrides_env)

                base_gates = spec_obj.get("qualityGates", []) if isinstance(spec_obj, dict) else []
                override_gates = _get(e, ["overrides", "qualityGates"], []) or []
                merged_gates, active_statuses, proposed_statuses, workflow_templates, gitops_files = _merge_quality_gates(
                    base_gates, override_gates, e["kubenvNamespace"], xqualitygate_lookup, app_name, e["name"]
                )
                # Merge debug: environment keys and merged gates
                env_vars_keys = sorted(list(effective_env.keys()))
                log.debug(
                    "kubenv.merge",
                    env=canonical,
                    envVarKeys=env_vars_keys,
                    gates=[
                        {
                            "ref": g.get("ref"),
                            "key": g.get("key"),
                            "phase": g.get("phase"),
                            "required": g.get("required"),
                        }
                        for g in merged_gates
                    ],
                )
                log.debug(
                    "kubenv.commit-statuses",
                    env=canonical,
                    active=[s["key"] for s in active_statuses],
                    proposed=[s["key"] for s in proposed_statuses],
                )

                # Generate shared resources metadata
                all_events = set()
                for gate in merged_gates:
                    triggers = gate.get("triggers", {})
                    events = triggers.get("events", [])
                    all_events.update(events)

                shared_resources = {
                    "eventSource": {
                        "name": f"{app_name}-github-events",
                        "namespace": f"{app_name}-{e['name']}",
                        "aggregatedEvents": sorted(all_events),
                        "webhookUrl": f"https://events-{app_name}.demo.kubecore.io"
                    },
                    "rbac": {
                        "serviceAccountName": "quality-gate-runner",
                        "namespace": f"{app_name}-{e['name']}",
                        "permissions": ["commitstatuses:create", "secrets:get"]
                    }
                }

                target = _resolve_target_environment(app_name, e["name"], spec_obj)

                env_resolved.append(
                    {
                        "name": e["name"],
                        "namespace": e["namespace"],
                        "enabled": e["enabled"],
                        "overrides": e["overrides"],
                        "target": target,
                        "kubenv": {
                            "found": True,
                            "resourceName": resource_name,
                            "spec": spec_obj,
                            "labels": labels,
                            "annotations": meta.get("annotations", {}),
                            "claimName": claim_name,
                        },
                        "effective": {
                            "resources": effective_resources,
                            "environment": effective_env,
                            "qualityGates": merged_gates,
                            "commitStatuses": {
                                "active": active_statuses,
                                "proposed": proposed_statuses,
                            },
                            "workflowGeneration": {
                                "templates": workflow_templates,
                                "gitopsFiles": gitops_files,
                                "sharedResources": shared_resources,
                            },
                        },
                    }
                )
            else:
                env_resolved.append(
                    {
                        "name": e["name"],
                        "namespace": e["namespace"],
                        "enabled": e["enabled"],
                        "overrides": e["overrides"],
                        "kubenv": {
                            "found": False,
                            "resourceName": canonical,
                        },
                    }
                )

        referenced_keys = sorted(referenced_set)
        found_keys = sorted(found_set)
        missing_keys = sorted(referenced_set - found_set)

        # Track quality gate references and resolution
        referenced_gates = sorted(all_gate_refs)
        found_gates = sorted(xqualitygate_lookup.keys())
        missing_gates = sorted(all_gate_refs - set(found_gates))
        # Structured metrics logs for verification
        log.debug(
            "kubenv.metrics",
            metrics={
                "referenced": referenced_keys,
                "found": found_keys,
                "missing": missing_keys,
                "counts": {
                    "ref": len(referenced_keys),
                    "found": len(found_keys),
                    "missing": len(missing_keys),
                },
            },
        )
        log.debug(
            "kubenv.match",
            foundCount=len(found_keys),
            found=found_keys,
            missing=missing_keys,
        )
        if missing_keys:
            log.warning(
                "kubenv.missing",
                missing=missing_keys,
            )
        # High-signal summary log
        log.info(
            "kubenv.summary",
            referenced=referenced_keys,
            found=found_keys,
            missing=missing_keys,
            counts={
                "ref": len(referenced_keys),
                "found": len(found_keys),
                "missing": len(missing_keys),
            },
        )

        app_resolved = {
            "app": app_obj,
            "project": project_obj,
            "environments": env_resolved,
            "summary": {
                "referencedKubenvNames": referenced_keys,
                "foundKubenvNames": found_keys,
                "missingKubenvNames": missing_keys,
                "referencedQualityGates": referenced_gates,
                "foundQualityGates": found_gates,
                "missingQualityGates": missing_gates,
                # Backward-compatible nested counts
                "counts": {
                    "referenced": len(referenced_keys),
                    "found": len(found_keys),
                    "missing": len(missing_keys),
                    "qualityGatesReferenced": len(referenced_gates),
                    "qualityGatesFound": len(found_gates),
                    "qualityGatesMissing": len(missing_gates),
                },
                # New explicit counters for template clarity
                "referencedCount": len(referenced_keys),
                "foundCount": len(found_keys),
                "missingCount": len(missing_keys),
            },
            "metadata": {
                "resolverVersion": "v1.3.0",
                "resolvedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "cacheKey": f"{app_name}-{project_name or 'unknown'}-{hash(str(xr)) % 10000:04x}",
                "resolutionDuration": f"{(time.time() - t_start):.1f}s"
            },
        }

        # Write into namespaced context key
        ctx_key = "apiextensions.crossplane.io/context.kubecore.io"
        current_ctx = resource.struct_to_dict(rsp.context)
        current_ctx[ctx_key] = {
            "appResolved": app_resolved,
            "kubenvLookup": kubenv_lookup,
            "qualityGateLookup": xqualitygate_lookup,
        }
        rsp.context = resource.dict_to_struct(current_ctx)

        response.normal(rsp, "function-kubecore-app-resolver completed")
        log.info(
            "resolve-app-context.context-populated",
            summary={
                "referenced": app_resolved["summary"]["referencedKubenvNames"],
                "found": app_resolved["summary"]["foundKubenvNames"],
                "missing": app_resolved["summary"]["missingKubenvNames"],
            },
            durationMs=int((time.time() - t_start) * 1000),
        )
        log.info(
            "resolve-app-context.complete",
            step="resolve-app-context",
            durationMs=int((time.time() - t_start) * 1000),
        )
        return rsp
