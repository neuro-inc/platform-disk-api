from typing import Any, Dict, List

import pytest

from platform_disk_api.kube_client import PodListResult, PodRead, PodWatchEvent


class TestPodSerialization:
    def _make_pod_payload(self, pvc_names: List[str]) -> Dict[str, Any]:
        return {
            "kind": "Pod",
            "apiVersion": "v1",
            "metadata": {"name": "boo"},
            "spec": {
                "automountServiceAccountToken": False,
                "containers": [
                    {
                        "name": "hello",
                        "image": "busybox",
                        "command": ["sh", "-c", "sleep 1"],
                    }
                ],
                "volumes": [
                    {"name": f"disk-{i}", "persistentVolumeClaim": {"claimName": name}}
                    for (i, name) in enumerate(pvc_names)
                ],
            },
        }

    @pytest.mark.parametrize("pvc_names", [("pvc1", "pvc2", "pvc3")])
    def test_pod_from_primitive(self, pvc_names: List[str],) -> None:
        pod = PodRead.from_primitive(self._make_pod_payload(pvc_names))

        assert set(pod.pvc_in_use) == set(pvc_names)

    @pytest.mark.parametrize(
        "resource_version,pvc_names", [("ver1", ("pvc1", "pvc2", "pvc3"))]
    )
    def test_pod_list_from_primitive(
        self, resource_version: str, pvc_names: List[str],
    ) -> None:
        result = PodListResult.from_primitive(
            {
                "metadata": {"resourceVersion": resource_version},
                "items": [self._make_pod_payload(pvc_names)],
            }
        )

        assert set(result.pods[0].pvc_in_use) == set(pvc_names)
        assert result.resource_version == resource_version

    @pytest.mark.parametrize("pvc_names", [("pvc1", "pvc2", "pvc3")])
    def test_pod_watch_event_from_primitive(self, pvc_names: List[str],) -> None:
        event = PodWatchEvent.from_primitive(
            {"type": "ADDED", "object": self._make_pod_payload(pvc_names)}
        )

        assert set(event.pod.pvc_in_use) == set(pvc_names)
        assert event.type == PodWatchEvent.Type.ADDED

    @pytest.mark.parametrize("resource_version", ["ver2"])
    def test_pod_watch_bookmark_event_from_primitive(
        self, resource_version: str,
    ) -> None:
        event = PodWatchEvent.from_primitive(
            {
                "type": "BOOKMARK",
                "object": {
                    "kind": "Pod",
                    "apiVersion": "v1",
                    "metadata": {"resourceVersion": resource_version},
                },
            }
        )

        assert event.type == PodWatchEvent.Type.BOOKMARK
        assert event.resource_version == resource_version
