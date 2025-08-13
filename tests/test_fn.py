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

    async def test_both_kubenves_found(self) -> None:
        @dataclasses.dataclass
        class TestCase:
            reason: str
            req: fnv1.RunFunctionRequest
            lister: object
            assert_fn: callable

        class DummyLister:
            def __init__(self, kubenv_items_by_name, project_items):
                self.kubenv_items_by_name = kubenv_items_by_name
                self.project_items = project_items

            def list_xkubenenvs_by_claim(self, name, namespace):  # noqa: ARG002
                return self.kubenv_items_by_name.get(name, [])

            def list_xgithubprojects_by_claim(self, name, namespace):  # noqa: ARG002
                return self.project_items

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
            "metadata": {"name": "demo-dev-slsf6", "labels": {"crossplane.io/claim-name": "demo-dev"}},
            "spec": {
                "environmentType": "dev",
                "resources": {
                    "profile": "small",
                    "defaults": {
                        "requests": {"cpu": "100m", "memory": "128Mi"},
                        "limits": {"cpu": "500m", "memory": "256Mi"},
                    },
                },
                "environmentConfig": {
                    "variables": {"ENVIRONMENT": "development"}
                },
                "qualityGates": ["checks"],
                "kubeClusterRef": {"name": "demo-cluster", "namespace": "test"},
            },
        }
        kubenv_item_v2 = {
            "metadata": {"name": "demo-dev-v2-aaaaa", "labels": {"crossplane.io/claim-name": "demo-dev-v2"}},
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

        lister = DummyLister(
            kubenv_items_by_name={
                "demo-dev": [kubenv_item],
                "demo-dev-v2": [kubenv_item_v2],
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
        self.assertEqual(
            resolved["project"].get("providerConfigs", {}).get("github"),
            "github-default",
        )
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

        class DummyLister:
            def list_xkubenenvs_by_claim(self, name, namespace):  # noqa: ARG002
                if name == "demo-dev":
                    return [
                        {
                            "metadata": {
                                "name": "demo-dev-xyz",
                                "labels": {"crossplane.io/claim-name": "demo-dev"},
                            },
                            "spec": {},
                        }
                    ]
                return []

            def list_xgithubprojects_by_claim(self, name, namespace):  # noqa: ARG002
                return []

        req = fnv1.RunFunctionRequest(
            observed=fnv1.State(
                composite=fnv1.Resource(resource=resource.dict_to_struct(xr)),
            )
        )
        runner = fn.FunctionRunner(lister=DummyLister())
        got = await runner.RunFunction(req, None)
        got_dict = json_format.MessageToDict(got)
        resolved = got_dict["context"][
            "apiextensions.crossplane.io/context.kubecore.io"
        ]["appResolved"]
        self.assertEqual(resolved["summary"]["counts"], {"referenced": 2, "found": 1, "missing": 1})
        envs = resolved["environments"]
        self.assertFalse(
            next(e for e in envs if e["name"] == "missing-env")["kubenv"]["found"]
        )

    async def test_no_environments(self) -> None:
        xr = {
            "apiVersion": "platform.kubecore.io/v1alpha1",
            "kind": "XApp",
            "metadata": {"name": "art-api"},
        }
        class DummyLister:
            def list_xkubenenvs_by_claim(self, name, namespace):  # noqa: ARG002
                return []
            def list_xgithubprojects_by_claim(self, name, namespace):  # noqa: ARG002
                return []
        req = fnv1.RunFunctionRequest(
            observed=fnv1.State(
                composite=fnv1.Resource(resource=resource.dict_to_struct(xr)),
            )
        )
        runner = fn.FunctionRunner(lister=DummyLister())
        got = await runner.RunFunction(req, None)
        got_dict = json_format.MessageToDict(got)
        resolved = got_dict["context"][
            "apiextensions.crossplane.io/context.kubecore.io"
        ]["appResolved"]
        self.assertEqual(resolved["environments"], [])
        self.assertEqual(
            resolved["summary"]["counts"],
            {"referenced": 0, "found": 0, "missing": 0},
        )

        # Ensure no unexpected errors when no environments are present


if __name__ == "__main__":
    unittest.main()
