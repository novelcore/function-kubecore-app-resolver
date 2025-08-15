import dataclasses
import unittest

from crossplane.function import logging, resource
from crossplane.function.proto.v1 import run_function_pb2 as fnv1
from google.protobuf import json_format

from function import fn


class TestFunctionRunner(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        # Allow larger diffs, since we diff large strings of JSON.
        self.maxDiff = 2000

        logging.configure(level=logging.Level.DISABLED)

    class DummyLister:
        def __init__(
            self,
            kubenv_items_by_ns=None,
            project_items=None,
            qualitygate_items_by_ns=None,
            xgithubapp_items=None,
        ):
            self.kubenv_items_by_ns = kubenv_items_by_ns or {}
            self.project_items = project_items or []
            self.qualitygate_items_by_ns = qualitygate_items_by_ns or {}
            self.xgithubapp_items = xgithubapp_items or []

        def list_kubenvs_in_namespace(self, namespace):
            return self.kubenv_items_by_ns.get(namespace, [])

        def list_xgithubprojects_by_claim(self, name, namespace):  # noqa: ARG002
            return self.project_items

        def list_qualitygates_in_namespace(self, namespace):
            return self.qualitygate_items_by_ns.get(namespace, [])

        def list_xqualitygates_by_claim(self, name, namespace):  # noqa: ARG002
            return []

        def list_xgithubapps_by_claim(self, name, namespace):  # noqa: ARG002
            return self.xgithubapp_items

    async def test_both_kubenves_found(self) -> None:
        @dataclasses.dataclass
        class TestCase:
            reason: str
            req: fnv1.RunFunctionRequest
            lister: object
            assert_fn: callable

        xr = {
            "apiVersion": "platform.kubecore.io/v1alpha1",
            "kind": "XApp",
            "metadata": {"name": "art-api"},
            "spec": {
                "type": "rest",
                "image": "ghcr.io/novelcore/art-api:latest",
                "port": 8000,
                "githubProjectRef": {"name": "demo-project", "namespace": "test"},
                "environments": [
                    {
                        "kubenvRef": {"name": "demo-dev", "namespace": "test"},
                        "enabled": True,
                        "overrides": {"replicas": 1},
                    },
                    {
                        "kubenvRef": {"name": "demo-dev-v2", "namespace": "test"},
                        "enabled": True,
                    },
                ],
            },
        }

        kubenv_item = {
            "apiVersion": "platform.kubecore.io/v1alpha1",
            "kind": "KubEnv",
            "metadata": {
                "name": "demo-dev",
                "namespace": "test",
                "labels": {"crossplane.io/claim-name": "demo-dev"},
            },
            "spec": {
                "environmentType": "dev",
                "resources": {
                    "profile": "small",
                    "defaults": {
                        "requests": {"cpu": "100m", "memory": "128Mi"},
                        "limits": {"cpu": "500m", "memory": "256Mi"},
                    },
                },
                "environmentConfig": {"variables": {"ENVIRONMENT": "development"}},
                "qualityGates": ["checks"],
                "kubeClusterRef": {"name": "demo-cluster", "namespace": "test"},
            },
        }
        kubenv_item_v2 = {
            "apiVersion": "platform.kubecore.io/v1alpha1",
            "kind": "KubEnv",
            "metadata": {
                "name": "demo-dev-v2",
                "namespace": "test",
                "labels": {"crossplane.io/claim-name": "demo-dev-v2"},
            },
            "spec": {"environmentType": "dev"},
        }

        project_item = {
            "metadata": {"name": "demo-project-4v4k4"},
            "status": {"providerConfig": {"github": "github-default"}},
        }

        req = fnv1.RunFunctionRequest(
            observed=fnv1.State(
                composite=fnv1.Resource(resource=resource.dict_to_struct(xr)),
            )
        )

        lister = self.DummyLister(
            kubenv_items_by_ns={
                "test": [kubenv_item, kubenv_item_v2],
            },
            project_items=[project_item],
        )

        runner = fn.FunctionRunner(lister=lister)
        got = await runner.RunFunction(req, None)
        got_dict = json_format.MessageToDict(got)

        ctx = got_dict.get("context", {}).get(
            "apiextensions.crossplane.io/context.kubecore.io", {}
        )
        self.assertIn("appResolved", ctx)
        resolved = ctx["appResolved"]
        self.assertEqual(resolved["app"]["name"], "art-api")
        self.assertEqual(resolved["project"]["name"], "demo-project")
        names = [e["name"] for e in resolved["environments"]]
        self.assertEqual(names, ["demo-dev", "demo-dev-v2"])  # first-wins keeps order
        self.assertEqual(resolved["summary"]["counts"]["missing"], 0)

    async def test_one_kubenv_missing(self) -> None:
        xr = {
            "apiVersion": "platform.kubecore.io/v1alpha1",
            "kind": "XApp",
            "metadata": {"name": "art-api"},
            "spec": {
                "environments": [
                    {"kubenvRef": {"name": "demo-dev"}},
                    {"kubenvRef": {"name": "missing-env"}},
                ],
            },
        }

        req = fnv1.RunFunctionRequest(
            observed=fnv1.State(
                composite=fnv1.Resource(resource=resource.dict_to_struct(xr)),
            )
        )

        lister = self.DummyLister(
            kubenv_items_by_ns={
                "default": [
                    {
                        "apiVersion": "platform.kubecore.io/v1alpha1",
                        "kind": "KubEnv",
                        "metadata": {
                            "name": "demo-dev",
                            "namespace": "default",
                            "labels": {"crossplane.io/claim-name": "demo-dev"},
                        },
                        "spec": {},
                    }
                ]
            }
        )
        runner = fn.FunctionRunner(lister=lister)
        got = await runner.RunFunction(req, None)
        got_dict = json_format.MessageToDict(got)
        ctx = got_dict["context"]["apiextensions.crossplane.io/context.kubecore.io"]
        resolved = ctx["appResolved"]
        counts = resolved["summary"]["counts"]
        self.assertEqual(counts["referenced"], 2)
        self.assertEqual(counts["found"], 1)
        self.assertEqual(counts["missing"], 1)
        # New fields should be present
        self.assertIn("qualityGatesReferenced", counts)
        self.assertIn("qualityGatesFound", counts)
        self.assertIn("qualityGatesMissing", counts)
        envs = resolved["environments"]
        self.assertFalse(
            next(e for e in envs if e["name"] == "missing-env")["kubenv"]["found"]
        )

        # Ensure kubenvLookup contains ONLY canonical keys and aliases are under
        # canonical keys only (no plain-name aliases)
        lookup = ctx.get("kubenvLookup", {})
        self.assertIn("default/demo-dev", lookup)
        self.assertNotIn("demo-dev", lookup)

    async def test_no_environments(self) -> None:
        xr = {
            "apiVersion": "platform.kubecore.io/v1alpha1",
            "kind": "XApp",
            "metadata": {"name": "art-api"},
        }

        req = fnv1.RunFunctionRequest(
            observed=fnv1.State(
                composite=fnv1.Resource(resource=resource.dict_to_struct(xr)),
            )
        )
        runner = fn.FunctionRunner(lister=self.DummyLister())
        got = await runner.RunFunction(req, None)
        got_dict = json_format.MessageToDict(got)
        resolved = got_dict["context"][
            "apiextensions.crossplane.io/context.kubecore.io"
        ]["appResolved"]
        self.assertEqual(resolved["environments"], [])
        counts = resolved["summary"]["counts"]
        self.assertEqual(counts["referenced"], 0)
        self.assertEqual(counts["found"], 0)
        self.assertEqual(counts["missing"], 0)
        # New fields should be present
        self.assertIn("qualityGatesReferenced", counts)
        self.assertIn("qualityGatesFound", counts)
        self.assertIn("qualityGatesMissing", counts)

        # Ensure no unexpected errors when no environments are present

    async def test_rbac_failure_in_one_namespace(self) -> None:
        xr = {
            "apiVersion": "platform.kubecore.io/v1alpha1",
            "kind": "XApp",
            "metadata": {"name": "art-api"},
            "spec": {
                "environments": [
                    {"kubenvRef": {"name": "dev-a", "namespace": "ns-a"}},
                    {"kubenvRef": {"name": "dev-b", "namespace": "ns-b"}},
                ],
            },
        }

        class ForbiddenError(Exception):
            def __init__(self):
                super().__init__("forbidden")
                self.status = 403

        class CustomLister(self.DummyLister):
            def list_kubenvs_in_namespace(self, namespace):
                if namespace == "ns-a":
                    raise ForbiddenError()
                return [
                    {
                        "apiVersion": "platform.kubecore.io/v1alpha1",
                        "kind": "KubEnv",
                        "metadata": {"name": "dev-b", "namespace": "ns-b"},
                        "spec": {"environmentType": "dev"},
                    }
                ]

        req = fnv1.RunFunctionRequest(
            observed=fnv1.State(
                composite=fnv1.Resource(resource=resource.dict_to_struct(xr)),
            )
        )
        runner = fn.FunctionRunner(lister=CustomLister())
        got = await runner.RunFunction(req, None)
        got_dict = json_format.MessageToDict(got)
        resolved = got_dict["context"][
            "apiextensions.crossplane.io/context.kubecore.io"
        ]["appResolved"]
        counts = resolved["summary"]["counts"]
        self.assertEqual(counts["referenced"], 2)
        self.assertEqual(counts["found"], 1)
        self.assertEqual(counts["missing"], 1)
        # New fields should be present
        self.assertIn("qualityGatesReferenced", counts)
        self.assertIn("qualityGatesFound", counts)
        self.assertIn("qualityGatesMissing", counts)

    async def test_multiple_namespaces_found(self) -> None:
        xr = {
            "apiVersion": "platform.kubecore.io/v1alpha1",
            "kind": "XApp",
            "metadata": {"name": "art-api"},
            "spec": {
                "environments": [
                    {"kubenvRef": {"name": "dev-a", "namespace": "ns-a"}},
                    {"kubenvRef": {"name": "dev-b", "namespace": "ns-b"}},
                ],
            },
        }

        req = fnv1.RunFunctionRequest(
            observed=fnv1.State(
                composite=fnv1.Resource(resource=resource.dict_to_struct(xr)),
            )
        )

        lister = self.DummyLister(
            kubenv_items_by_ns={
                "ns-a": [
                    {
                        "apiVersion": "platform.kubecore.io/v1alpha1",
                        "kind": "KubEnv",
                        "metadata": {"name": "dev-a", "namespace": "ns-a"},
                        "spec": {"environmentType": "dev"},
                    }
                ],
                "ns-b": [
                    {
                        "apiVersion": "platform.kubecore.io/v1alpha1",
                        "kind": "KubEnv",
                        "metadata": {"name": "dev-b", "namespace": "ns-b"},
                        "spec": {"environmentType": "dev"},
                    }
                ]
            }
        )
        runner = fn.FunctionRunner(lister=lister)
        got = await runner.RunFunction(req, None)
        got_dict = json_format.MessageToDict(got)
        resolved = got_dict["context"][
            "apiextensions.crossplane.io/context.kubecore.io"
        ]["appResolved"]
        counts = resolved["summary"]["counts"]
        self.assertEqual(counts["referenced"], 2)
        self.assertEqual(counts["found"], 2)
        self.assertEqual(counts["missing"], 0)
        # New fields should be present
        self.assertIn("qualityGatesReferenced", counts)
        self.assertIn("qualityGatesFound", counts)
        self.assertIn("qualityGatesMissing", counts)

    async def test_commit_statuses_from_gates_single_env(self) -> None:
        xr = {
            "apiVersion": "platform.kubecore.io/v1alpha1",
            "kind": "XApp",
            "metadata": {"name": "art-api"},
            "spec": {
                "environments": [
                    {
                        "kubenvRef": {"name": "demo-dev", "namespace": "test"},
                        "enabled": True,
                    }
                ]
            },
        }

        kubenv_item = {
            "apiVersion": "platform.kubecore.io/v1alpha1",
            "kind": "KubEnv",
            "metadata": {
                "name": "demo-dev",
                "namespace": "test",
                "labels": {"crossplane.io/claim-name": "demo-dev"},
            },
            "spec": {
                "resources": {
                    "defaults": {
                        "requests": {"cpu": "100m", "memory": "128Mi"},
                        "limits": {"cpu": "500m", "memory": "256Mi"},
                    }
                },
                "environmentConfig": {"variables": {"ENVIRONMENT": "development"}},
                "qualityGates": [
                    {
                        "ref": {"name": "smoke-test-gate", "namespace": "test"},
                        "key": "smoke-test",
                        "phase": "active",
                        "required": True,
                    },
                    {
                        "ref": {"name": "security-scan-gate", "namespace": "test"},
                        "key": "security-scan",
                        "phase": "proposed",
                        "required": True,
                    },
                ],
            },
        }

        req = fnv1.RunFunctionRequest(
            observed=fnv1.State(
                composite=fnv1.Resource(resource=resource.dict_to_struct(xr)),
            )
        )

        lister = self.DummyLister(
            kubenv_items_by_ns={"test": [kubenv_item]}
        )
        runner = fn.FunctionRunner(lister=lister)
        got = await runner.RunFunction(req, None)
        got_dict = json_format.MessageToDict(got)
        resolved_envs = got_dict["context"][
            "apiextensions.crossplane.io/context.kubecore.io"
        ]["appResolved"]["environments"]
        self.assertEqual(len(resolved_envs), 1)
        eff = resolved_envs[0]["effective"]
        commit_statuses = eff["commitStatuses"]
        self.assertIn("active", commit_statuses)
        self.assertIn("proposed", commit_statuses)
        # Check that we have the expected keys (structure may be enhanced)
        active_keys = [s.get("key") for s in commit_statuses["active"]]
        proposed_keys = [s.get("key") for s in commit_statuses["proposed"]]
        self.assertIn("smoke-test", active_keys)
        self.assertIn("security-scan", proposed_keys)
        # Ensure quality gates echo back the merged list with required fields
        gates = eff["qualityGates"]
        self.assertEqual(len(gates), 2)
        self.assertEqual(gates[0]["ref"]["name"], "smoke-test-gate")
        self.assertEqual(gates[0]["phase"], "active")
        self.assertTrue(gates[0]["required"])

    async def test_gate_merge_precedence_overrides_win(self) -> None:
        xr = {
            "apiVersion": "platform.kubecore.io/v1alpha1",
            "kind": "XApp",
            "metadata": {"name": "art-api"},
            "spec": {
                "environments": [
                    {
                        "kubenvRef": {"name": "demo-dev", "namespace": "test"},
                        "enabled": True,
                        "overrides": {
                            "qualityGates": [
                                {
                                    "ref": {
                                        "name": "smoke-test-gate",
                                        "namespace": "test",
                                    },
                                    "key": "smoke-override",
                                    "phase": "proposed",
                                    "required": False,
                                }
                            ]
                        },
                    }
                ]
            },
        }
        kubenv_item = {
            "apiVersion": "platform.kubecore.io/v1alpha1",
            "kind": "KubEnv",
            "metadata": {
                "name": "demo-dev",
                "namespace": "test",
                "labels": {"crossplane.io/claim-name": "demo-dev"},
            },
            "spec": {
                "qualityGates": [
                    {
                        "ref": {"name": "smoke-test-gate", "namespace": "test"},
                        "key": "smoke-test",
                        "phase": "active",
                        "required": True,
                    }
                ]
            },
        }

        req = fnv1.RunFunctionRequest(
            observed=fnv1.State(
                composite=fnv1.Resource(resource=resource.dict_to_struct(xr)),
            )
        )

        lister = self.DummyLister(
            kubenv_items_by_ns={"test": [kubenv_item]}
        )
        runner = fn.FunctionRunner(lister=lister)
        got = await runner.RunFunction(req, None)
        got_dict = json_format.MessageToDict(got)
        eff = got_dict["context"][
            "apiextensions.crossplane.io/context.kubecore.io"
        ]["appResolved"]["environments"][0]["effective"]
        # Override wins for key/phase/required
        gates = eff["qualityGates"]
        self.assertEqual(len(gates), 1)
        self.assertEqual(gates[0]["key"], "smoke-override")
        self.assertEqual(gates[0]["phase"], "proposed")
        self.assertFalse(gates[0]["required"])  # override set False
        # Commit-status reflects override
        commit_statuses = eff["commitStatuses"]
        self.assertIn("active", commit_statuses)
        self.assertIn("proposed", commit_statuses)
        active_keys = [s.get("key") for s in commit_statuses["active"]]
        proposed_keys = [s.get("key") for s in commit_statuses["proposed"]]
        self.assertEqual(len(active_keys), 0)
        self.assertIn("smoke-override", proposed_keys)

    async def test_enhanced_quality_gates_with_embedded_workflow(self) -> None:
        """Test the enhanced quality gate processing with embedded workflow schema."""
        xr = {
            "apiVersion": "platform.kubecore.io/v1alpha1",
            "kind": "XApp",
            "metadata": {"name": "test-app"},
            "spec": {
                "type": "rest",
                "image": "test-image:latest",
                "port": 8000,
                "githubProjectRef": {"name": "test-project", "namespace": "test"},
                "environments": [
                    {
                        "kubenvRef": {"name": "test-dev", "namespace": "test"},
                        "enabled": True,
                    }
                ],
            },
        }

        kubenv_item = {
            "apiVersion": "platform.kubecore.io/v1alpha1",
            "kind": "KubEnv",
            "metadata": {
                "name": "test-dev",
                "namespace": "test",
                "labels": {"crossplane.io/claim-name": "test-dev"},
            },
            "spec": {
                "environmentType": "dev",
                "resources": {
                    "defaults": {
                        "requests": {"cpu": "100m", "memory": "128Mi"},
                        "limits": {"cpu": "500m", "memory": "256Mi"},
                    }
                },
                "environmentConfig": {"variables": {"ENVIRONMENT": "development"}},
                "qualityGates": [
                    {
                        "ref": {"name": "security-scan-gate", "namespace": "test"},
                        "key": "security-scan",
                        "phase": "proposed",
                        "required": True,
                    }
                ],
                "kubeClusterRef": {"name": "demo-cluster", "namespace": "test"},
            },
        }

        qualitygate_item = {
            "apiVersion": "platform.kubecore.io/v1alpha1",
            "kind": "QualityGate",
            "metadata": {
                "name": "security-scan-gate",
                "namespace": "test",
            },
            "spec": {
                "key": "enhanced-security-scan",  # This should be extracted
                "description": "Static analysis and dependency audit",
                "category": "security",
                "severity": "high",
                "workflowSchema": {
                    "serviceAccountName": "quality-gate-runner",
                    "parameters": [
                        {
                            "name": "commit-sha",
                            "description": "Git commit SHA to validate",
                            "required": True,
                            "type": "string"
                        }
                    ],
                    "steps": [
                        {
                            "name": "security-scan",
                            "container": {
                                "image": "security-scanner:latest",
                                "command": ["scan"],
                                "args": ["--commit={{workflow.parameters.commit-sha}}"],
                                "env": [
                                    {
                                        "name": "GITHUB_TOKEN",
                                        "valueFrom": {
                                            "secretKeyRef": {
                                                "name": "github-credentials",
                                                "key": "token"
                                            }
                                        }
                                    }
                                ],
                                "resources": {
                                    "requests": {"memory": "256Mi", "cpu": "250m"},
                                    "limits": {"memory": "512Mi", "cpu": "500m"}
                                }
                            }
                        }
                    ],
                    "timeout": "10m",
                    "outputs": {
                        "parameters": [
                            {
                                "name": "quality-gate-result",
                                "description": "Overall quality gate result"
                            }
                        ]
                    }
                },
                "triggers": {
                    "provider": "github",
                    "events": ["deployment", "pull_request"],
                    "filters": [
                        {
                            "path": "body.action",
                            "type": "string",
                            "values": ["created", "synchronize"]
                        }
                    ]
                },
                "commitStatus": {
                    "descriptionTemplate": "Security scan {{.environment}}: {{.summary}}",
                    "urlTemplate": "https://argo.{{.cluster-domain}}/workflows/{{.namespace}}/{{.workflow-name}}",
                    "resultMapping": {
                        "outputParameter": "quality-gate-result",
                        "successValues": ["success", "passed", "clean"],
                        "failureValues": ["failure", "failed", "vulnerable"]
                    }
                }
            },
        }

        project_item = {
            "metadata": {"name": "test-project-4v4k4"},
            "status": {
                "providerConfig": {"github": "github-default"},
                "repository": {
                    "owner": "testorg",
                    "name": "test-app",
                    "fullName": "testorg/test-app"
                }
            },
        }

        github_app_item = {
            "metadata": {"name": "test-github-app", "namespace": "test"},
            "status": {
                "providerConfig": {"github": "github-default"},
                "githubProjectRef": {"name": "test-project"}
            },
        }

        req = fnv1.RunFunctionRequest(
            observed=fnv1.State(
                composite=fnv1.Resource(resource=resource.dict_to_struct(xr)),
            )
        )

        lister = self.DummyLister(
            kubenv_items_by_ns={"test": [kubenv_item]},
            project_items=[project_item],
            qualitygate_items_by_ns={"test": [qualitygate_item]},
            xgithubapp_items=[github_app_item],
        )

        runner = fn.FunctionRunner(lister=lister)
        got = await runner.RunFunction(req, None)
        got_dict = json_format.MessageToDict(got)

        ctx = got_dict.get("context", {}).get(
            "apiextensions.crossplane.io/context.kubecore.io", {}
        )
        self.assertIn("appResolved", ctx)
        self.assertIn("qualityGateLookup", ctx)

        resolved = ctx["appResolved"]

        # Test enhanced project structure with GitHub integration
        project = resolved["project"]
        self.assertEqual(project["name"], "test-project")
        self.assertIn("github", project)
        self.assertTrue(project["github"]["app"]["found"])
        self.assertEqual(project["github"]["repository"]["owner"], "testorg")
        self.assertEqual(project["github"]["repository"]["name"], "test-app")

        # Test enhanced environment structure
        env = resolved["environments"][0]
        self.assertIn("target", env)
        self.assertEqual(env["target"]["namespace"], "test-app-test-dev")
        self.assertEqual(env["target"]["cluster"], "demo-cluster.eks.eu-west-3.amazonaws.com")

        # Test enhanced effective section
        effective = env["effective"]
        self.assertIn("qualityGates", effective)
        self.assertIn("commitStatuses", effective)
        self.assertIn("workflowGeneration", effective)

        # Test quality gates with embedded workflow schema
        gates = effective["qualityGates"]
        self.assertEqual(len(gates), 1)
        gate = gates[0]
        self.assertEqual(gate["key"], "enhanced-security-scan")  # Extracted from QualityGate.spec.key
        self.assertEqual(gate["description"], "Static analysis and dependency audit")
        self.assertEqual(gate["category"], "security")
        self.assertEqual(gate["severity"], "high")
        self.assertIn("parameters", gate)  # Parameters from KubEnv configuration
        self.assertIn("workflowSchema", gate)
        self.assertIn("triggers", gate)
        self.assertIn("commitStatus", gate)

        # Test workflow generation metadata
        workflow_gen = effective["workflowGeneration"]
        self.assertIn("templates", workflow_gen)
        self.assertIn("gitopsFiles", workflow_gen)
        self.assertIn("sharedResources", workflow_gen)

        templates = workflow_gen["templates"]
        self.assertEqual(len(templates), 1)
        template = templates[0]
        self.assertEqual(template["gateName"], "enhanced-security-scan")
        self.assertEqual(template["templateName"], "enhanced-security-scan-template")
        self.assertTrue(template["generationRequired"])
        self.assertIn("validationStatus", template)
        self.assertIn("metadata", template)

        # Test GitOps files
        gitops_files = workflow_gen["gitopsFiles"]
        self.assertEqual(len(gitops_files), 2)  # WorkflowTemplate and Sensor

        # Test enhanced commit statuses
        commit_statuses = effective["commitStatuses"]
        self.assertIn("active", commit_statuses)
        self.assertIn("proposed", commit_statuses)
        proposed = commit_statuses["proposed"]
        self.assertEqual(len(proposed), 1)
        status = proposed[0]
        self.assertEqual(status["key"], "enhanced-security-scan")
        self.assertIn("description", status)
        self.assertIn("context", status)
        self.assertIn("targetUrl", status)

        # Test summary with quality gate tracking
        summary = resolved["summary"]
        self.assertIn("referencedQualityGates", summary)
        self.assertIn("foundQualityGates", summary)
        self.assertIn("missingQualityGates", summary)
        self.assertEqual(len(summary["referencedQualityGates"]), 1)
        self.assertEqual(len(summary["foundQualityGates"]), 1)

        # Test metadata section
        self.assertIn("metadata", resolved)
        metadata = resolved["metadata"]
        self.assertIn("resolverVersion", metadata)
        self.assertIn("resolvedAt", metadata)
        self.assertIn("cacheKey", metadata)
        self.assertIn("resolutionDuration", metadata)

    async def test_quality_gate_validation_errors(self) -> None:
        """Test quality gate validation with invalid workflow schemas."""
        xr = {
            "apiVersion": "platform.kubecore.io/v1alpha1",
            "kind": "XApp",
            "metadata": {"name": "test-app"},
            "spec": {
                "environments": [
                    {
                        "kubenvRef": {"name": "test-dev", "namespace": "test"},
                        "enabled": True,
                    }
                ],
            },
        }

        kubenv_item = {
            "apiVersion": "platform.kubecore.io/v1alpha1",
            "kind": "KubEnv",
            "metadata": {
                "name": "test-dev",
                "namespace": "test",
                "labels": {"crossplane.io/claim-name": "test-dev"},
            },
            "spec": {
                "qualityGates": [
                    {
                        "ref": {"name": "invalid-gate", "namespace": "test"},
                        "key": "invalid-gate",
                        "phase": "active",
                        "required": True,
                    }
                ],
            },
        }

        # Quality gate with invalid workflow schema
        invalid_qualitygate_item = {
            "apiVersion": "platform.kubecore.io/v1alpha1",
            "kind": "QualityGate",
            "metadata": {
                "name": "invalid-gate",
                "namespace": "test",
            },
            "spec": {
                "description": "Invalid gate for testing",
                "workflowSchema": {
                    # Missing required fields like steps
                    "parameters": [
                        {"name": ""},  # Invalid parameter with empty name
                    ],
                    "steps": [],  # Empty steps array
                    "outputs": "invalid",  # Invalid outputs structure
                }
            },
        }

        req = fnv1.RunFunctionRequest(
            observed=fnv1.State(
                composite=fnv1.Resource(resource=resource.dict_to_struct(xr)),
            )
        )

        lister = self.DummyLister(
            kubenv_items_by_ns={"test": [kubenv_item]},
            project_items=[],
            qualitygate_items_by_ns={"test": [invalid_qualitygate_item]},
        )

        runner = fn.FunctionRunner(lister=lister)
        got = await runner.RunFunction(req, None)
        got_dict = json_format.MessageToDict(got)

        ctx = got_dict["context"]["apiextensions.crossplane.io/context.kubecore.io"]
        resolved = ctx["appResolved"]

        # Check that validation errors are captured
        env = resolved["environments"][0]
        workflow_gen = env["effective"]["workflowGeneration"]
        templates = workflow_gen["templates"]

        if templates:  # If template was generated despite errors
            template = templates[0]
            validation = template["validationStatus"]
            self.assertFalse(validation["parametersValid"] or validation["stepsValid"] or validation["outputsValid"])
            self.assertTrue(len(validation["errors"]) > 0)

    async def test_missing_quality_gates_handling(self) -> None:
        """Test handling of missing quality gate resources."""
        xr = {
            "apiVersion": "platform.kubecore.io/v1alpha1",
            "kind": "XApp",
            "metadata": {"name": "test-app"},
            "spec": {
                "environments": [
                    {
                        "kubenvRef": {"name": "test-dev", "namespace": "test"},
                        "enabled": True,
                    }
                ],
            },
        }

        kubenv_item = {
            "apiVersion": "platform.kubecore.io/v1alpha1",
            "kind": "KubEnv",
            "metadata": {
                "name": "test-dev",
                "namespace": "test",
                "labels": {"crossplane.io/claim-name": "test-dev"},
            },
            "spec": {
                "qualityGates": [
                    {
                        "ref": {"name": "missing-gate", "namespace": "test"},
                        "key": "missing-gate",
                        "phase": "active",
                        "required": True,
                    }
                ],
            },
        }

        req = fnv1.RunFunctionRequest(
            observed=fnv1.State(
                composite=fnv1.Resource(resource=resource.dict_to_struct(xr)),
            )
        )

        lister = self.DummyLister(
            kubenv_items_by_ns={"test": [kubenv_item]},
            project_items=[],
            qualitygate_items_by_ns={},  # No quality gates available
        )

        runner = fn.FunctionRunner(lister=lister)
        got = await runner.RunFunction(req, None)
        got_dict = json_format.MessageToDict(got)

        ctx = got_dict["context"]["apiextensions.crossplane.io/context.kubecore.io"]
        resolved = ctx["appResolved"]

        # Check that missing quality gates are tracked
        summary = resolved["summary"]
        self.assertEqual(len(summary["referencedQualityGates"]), 1)
        self.assertEqual(len(summary["foundQualityGates"]), 0)
        self.assertEqual(len(summary["missingQualityGates"]), 1)
        self.assertIn("test/missing-gate", summary["missingQualityGates"])

        # Quality gate should still be in the list but without embedded schema
        env = resolved["environments"][0]
        gates = env["effective"]["qualityGates"]
        self.assertEqual(len(gates), 1)
        gate = gates[0]
        self.assertEqual(gate["key"], "missing-gate")
        self.assertEqual(gate["workflowSchema"], {})  # Empty since gate not found

    async def test_quality_gate_key_fallback_behavior(self) -> None:
        """Test that key falls back to KubEnv configuration when QualityGate.spec.key is missing."""
        xr = {
            "apiVersion": "platform.kubecore.io/v1alpha1",
            "kind": "XApp",
            "metadata": {"name": "test-app"},
            "spec": {
                "environments": [
                    {
                        "kubenvRef": {"name": "test-dev", "namespace": "test"},
                        "enabled": True,
                    }
                ],
            },
        }

        kubenv_item = {
            "apiVersion": "platform.kubecore.io/v1alpha1",
            "kind": "KubEnv",
            "metadata": {
                "name": "test-dev",
                "namespace": "test",
                "labels": {"crossplane.io/claim-name": "test-dev"},
            },
            "spec": {
                "qualityGates": [
                    {
                        "ref": {"name": "fallback-gate", "namespace": "test"},
                        "key": "kubenv-fallback-key",  # This should be used as fallback
                        "phase": "active",
                        "required": True,
                    }
                ],
            },
        }

        # Quality gate without spec.key - should fallback to KubEnv key
        qualitygate_item = {
            "apiVersion": "platform.kubecore.io/v1alpha1",
            "kind": "QualityGate",
            "metadata": {
                "name": "fallback-gate",
                "namespace": "test",
            },
            "spec": {
                # No "key" field - should fallback to KubEnv configuration
                "description": "Gate without spec.key for fallback testing",
                "category": "testing",
                "severity": "low",
            },
        }

        req = fnv1.RunFunctionRequest(
            observed=fnv1.State(
                composite=fnv1.Resource(resource=resource.dict_to_struct(xr)),
            )
        )

        lister = self.DummyLister(
            kubenv_items_by_ns={"test": [kubenv_item]},
            project_items=[],
            qualitygate_items_by_ns={"test": [qualitygate_item]},
        )

        runner = fn.FunctionRunner(lister=lister)
        got = await runner.RunFunction(req, None)
        got_dict = json_format.MessageToDict(got)

        ctx = got_dict["context"]["apiextensions.crossplane.io/context.kubecore.io"]
        resolved = ctx["appResolved"]

        # Check that fallback key is used
        env = resolved["environments"][0]
        gates = env["effective"]["qualityGates"]
        self.assertEqual(len(gates), 1)
        gate = gates[0]
        self.assertEqual(gate["key"], "kubenv-fallback-key")  # Should use KubEnv key as fallback
        self.assertEqual(gate["description"], "Gate without spec.key for fallback testing")
        self.assertEqual(gate["category"], "testing")


if __name__ == "__main__":
    unittest.main()
