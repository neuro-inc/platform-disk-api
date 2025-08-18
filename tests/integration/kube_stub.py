from __future__ import annotations
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any
import json

import aiohttp
import aiohttp.web
import pytest
from yarl import URL

STATE = {
    "namespaces": set(),
    "pvcs": {},  # key: (ns) -> set of pvc names
    "storageclasses": set(),
}


def _json(data: Any, status: int = 200) -> aiohttp.web.Response:
    return aiohttp.web.Response(
        text=json.dumps(data), status=status, content_type="application/json"
    )


async def make_kube_stub_app(k8s_storage_class: str) -> aiohttp.web.Application:
    app = aiohttp.web.Application()

    STATE["storageclasses"].add(k8s_storage_class)

    async def ping(request: aiohttp.web.Request) -> aiohttp.web.Response:
        return _json({"status": "ok"})

    # Namespaces
    async def list_namespaces(request: aiohttp.web.Request) -> aiohttp.web.Response:
        items = [{"metadata": {"name": n}} for n in sorted(STATE["namespaces"])]
        return _json({"items": items})

    async def create_namespace(request: aiohttp.web.Request) -> aiohttp.web.Response:
        body = await request.json()
        name = body.get("metadata", {}).get("name", "default")
        STATE["namespaces"].add(name)
        STATE["pvcs"].setdefault(name, set())
        return _json({"metadata": {"name": name}}, status=201)

    async def get_namespace(request: aiohttp.web.Request) -> aiohttp.web.Response:
        ns = request.match_info["namespace"]
        if ns in STATE["namespaces"]:
            return _json({"metadata": {"name": ns}})
        return _json({"kind": "Status", "status": "Failure"}, status=404)

    async def delete_namespace(request: aiohttp.web.Request) -> aiohttp.web.Response:
        ns = request.match_info["namespace"]
        STATE["namespaces"].discard(ns)
        STATE["pvcs"].pop(ns, None)
        return _json({"status": "Success"})

    # PVCs
    async def list_pvc(request: aiohttp.web.Request) -> aiohttp.web.Response:
        ns = request.match_info["namespace"]
        names = sorted(STATE["pvcs"].get(ns, set()))
        items = [{"metadata": {"name": n, "namespace": ns}} for n in names]
        return _json({"items": items})

    async def create_pvc(request: aiohttp.web.Request) -> aiohttp.web.Response:
        ns = request.match_info["namespace"]
        body = await request.json()
        name = body.get("metadata", {}).get("name", "pvc")
        STATE["namespaces"].add(ns)
        STATE["pvcs"].setdefault(ns, set()).add(name)
        return _json({"metadata": {"name": name, "namespace": ns}}, status=201)

    async def delete_pvc(request: aiohttp.web.Request) -> aiohttp.web.Response:
        ns = request.match_info["namespace"]
        name = request.match_info["name"]
        if name in STATE["pvcs"].get(ns, set()):
            STATE["pvcs"][ns].discard(name)
            return _json({"status": "Success"})
        return _json({"kind": "Status", "status": "Failure"}, status=404)

    async def list_all_pvcs(request: aiohttp.web.Request) -> aiohttp.web.Response:
        items = []
        for ns, names in STATE["pvcs"].items():
            for name in names:
                items.append({"metadata": {"name": name, "namespace": ns}})
        return _json({"items": items})

    async def list_sc(request: aiohttp.web.Request) -> aiohttp.web.Response:
        items = [{"metadata": {"name": n}} for n in sorted(STATE["storageclasses"])]
        return _json({"items": items})

    app.router.add_get("/ping", ping)

    app.router.add_get("/api/v1/namespaces", list_namespaces)
    app.router.add_post("/api/v1/namespaces", create_namespace)
    app.router.add_get("/api/v1/namespaces/{namespace}", get_namespace)
    app.router.add_delete("/api/v1/namespaces/{namespace}", delete_namespace)

    app.router.add_get(
        "/api/v1/namespaces/{namespace}/persistentvolumeclaims", list_pvc
    )
    app.router.add_post(
        "/api/v1/namespaces/{namespace}/persistentvolumeclaims", create_pvc
    )
    app.router.add_delete(
        "/api/v1/namespaces/{namespace}/persistentvolumeclaims/{name}", delete_pvc
    )

    app.router.add_get("/apis/storage.k8s.io/v1/storageclasses", list_sc)

    app.router.add_get("/api/v1/persistentvolumeclaims", list_all_pvcs)

    return app


@dataclass(frozen=True)
class _Addr:
    url: URL
    host: str
    port: int


@pytest.fixture(scope="function")
async def kube_api_stub(event_loop, k8s_storage_class: str) -> AsyncIterator[_Addr]:
    app = await make_kube_stub_app(k8s_storage_class)
    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    site = aiohttp.web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    sockets = site._server.sockets  # type: ignore[attr-defined]
    port = sockets[0].getsockname()[1]
    addr = _Addr(url=URL(f"http://127.0.0.1:{port}"), host="127.0.0.1", port=port)
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(str(addr.url / "ping"), timeout=2) as r:
                assert r.status == 200
        yield addr
    finally:
        await runner.cleanup()
