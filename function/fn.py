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
    """Resolve XGitHubProject by claim labels.

    Returns a tuple of (project dict, warning string or None).
    """
    project: dict[str, Any] = {
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
        # Do not fetch external project resources; only echo minimal reference data
        project_obj = {
            "name": project_name,
            "namespace": project_namespace,
            "providerConfigs": {},
        }

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

        # Build a temporary lookup of all claims by canonical key (used only for matching referenced envs)
        def _sanitize_kubenv_claim(obj: dict[str, Any]) -> dict[str, Any]:
            meta = obj.get("metadata", {}) if isinstance(obj, dict) else {}
            spec_obj = obj.get("spec", {}) if isinstance(obj, dict) else {}
            # Keep only fields needed downstream in sanitized spec
            sanitized_spec = {
                "resources": spec_obj.get("resources", {}),
                "environmentConfig": spec_obj.get("environmentConfig", {}),
                "qualityGates": spec_obj.get("qualityGates", []),
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
        ) -> tuple[list[dict[str, Any]], list[dict[str, str]], list[dict[str, str]]]:
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
            active_keys: set[str] = set()
            proposed_keys: set[str] = set()
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
                gate_out = {
                    "ref": ref_out,
                    "key": chosen_key,
                    "phase": phase,
                    "required": required_val,
                }
                merged_list.append(gate_out)
                # Build commit status sets by phase for non-empty keys only
                if isinstance(chosen_key, str) and chosen_key:
                    if phase == "active":
                        active_keys.add(chosen_key)
                    elif phase == "proposed":
                        proposed_keys.add(chosen_key)

            active_statuses = [{"key": k} for k in sorted(active_keys)]
            proposed_statuses = [{"key": k} for k in sorted(proposed_keys)]
            return merged_list, active_statuses, proposed_statuses

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
                merged_gates, active_statuses, proposed_statuses = _merge_quality_gates(
                    base_gates, override_gates, e["kubenvNamespace"]
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
                env_resolved.append(
                    {
                        "name": e["name"],
                        "namespace": e["namespace"],
                        "enabled": e["enabled"],
                        "overrides": e["overrides"],
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
                                "activeCommitStatuses": active_statuses,
                                "proposedCommitStatuses": proposed_statuses,
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
                # Backward-compatible nested counts
                "counts": {
                    "referenced": len(referenced_keys),
                    "found": len(found_keys),
                    "missing": len(missing_keys),
                },
                # New explicit counters for template clarity
                "referencedCount": len(referenced_keys),
                "foundCount": len(found_keys),
                "missingCount": len(missing_keys),
            },
        }

        # Write into namespaced context key
        ctx_key = "apiextensions.crossplane.io/context.kubecore.io"
        current_ctx = resource.struct_to_dict(rsp.context)
        current_ctx[ctx_key] = {
            "appResolved": app_resolved,
            "kubenvLookup": kubenv_lookup,
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
