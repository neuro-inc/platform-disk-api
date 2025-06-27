from __future__ import annotations

import base64
import json
import logging
import re
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from aiohttp import web
from aiohttp.web_exceptions import HTTPBadRequest
from apolo_kube_client.errors import ResourceExists

from ..api import create_kube_client
from ..config import Config
from ..kube_client import KubeClient, DiskNaming
from ..service import (
    DISK_API_MARK_LABEL,
    APOLO_DISK_API_MARK_LABEL,
    APOLO_DISK_API_NAME_ANNOTATION,
    DISK_API_NAME_ANNOTATION,
    Service,
    DISK_API_ORG_LABEL,
    APOLO_ORG_LABEL,
    DISK_API_PROJECT_LABEL,
    APOLO_PROJECT_LABEL,
    APOLO_DISK_API_CREATED_AT_ANNOTATION,
    DISK_API_CREATED_AT_ANNOTATION,
    USER_LABEL,
    APOLO_USER_LABEL,
    DiskNameUsed,
)
from ..utils import datetime_dump, utc_now

LOGGER = logging.getLogger(__name__)

PATH_ANNOTATIONS = "/metadata/annotations"
PATH_LABELS = "/metadata/labels"

# endswith dash and number, e.g.: -0, -1, -2, etc
RE_STATEFUL_SET_PVC_NAME_INDEX = re.compile(r"(?P<index>-\d+$)")

KUBE_CLIENT_KEY = web.AppKey("kube_client", KubeClient)
CONFIG_KEY = web.AppKey("config", Config)


def escape_json_pointer(path: str) -> str:
    """
    Escapes ~ and / in a JSON Pointer path according to RFC 6901.
    Replaces ~ with ~0 and / with ~1.
    """
    return path.replace("~", "~0").replace("/", "~1")


class AdmissionReviewPatchType(StrEnum):
    JSON = "JSONPatch"


@dataclass
class AdmissionReviewResponse:
    uid: str
    patch: list[dict[str, Any]] = field(default_factory=list)

    def add_patch(self, path: str, value: Any) -> None:
        self.patch.append(
            {
                "op": "add",
                "path": path,
                "value": value,
            }
        )

    def allow(self) -> web.Response:
        return web.json_response(self._to_primitive(allowed=True))

    def decline(self, status_code: int, message: str) -> web.Response:
        return web.json_response(
            self._to_primitive(allowed=False, status_code=status_code, message=message)
        )

    def _to_primitive(
        self,
        *,
        allowed: bool,
        status_code: int | None = None,
        message: str | None = None,
    ) -> dict[str, Any]:
        response = {"uid": self.uid, "allowed": allowed}
        if allowed and self.patch:
            dumped = json.dumps(self.patch).encode()
            patch = base64.b64encode(dumped).decode()
            response.update(
                {
                    "patch": patch,
                    "patchType": AdmissionReviewPatchType.JSON.value,
                }
            )
        elif status_code and message:
            response["status"] = {
                "code": status_code,
                "message": message,
            }

        return {
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "response": response,
        }


class AdmissionControllerHandler:
    def __init__(self, app: web.Application) -> None:
        self._app = app
        self._storage_class_name: str | None = self._config.disk.k8s_storage_class
        self._kind_handlers: dict[
            str,
            Callable[
                [dict[str, Any], str, AdmissionReviewResponse], Awaitable[web.Response]
            ],
        ] = {
            "Pod": self._handle_pod,
            "PersistentVolumeClaim": self._handle_pvc,
        }

    async def register(self) -> None:
        self._app.add_routes(
            [
                web.get("/ping", self.handle_ping),
                web.post("/mutate", self.handle_post_mutate),
            ]
        )
        self._storage_class_name = (
            self._storage_class_name
            or await self._kube_client.get_default_storage_class_name()
        )
        if not self._storage_class_name:
            raise RuntimeError(
                "unable to start an admission controller without a known storage class"
            )
        LOGGER.info(
            f"initialized disks admission controller with the storage class `{self._storage_class_name}`"
        )

    @property
    def _kube_client(self) -> KubeClient:
        return self._app[KUBE_CLIENT_KEY]

    @property
    def _config(self) -> Config:
        return self._app[CONFIG_KEY]

    async def handle_ping(self, request: web.Request) -> web.Response:
        return web.Response(text="ok")

    async def handle_post_mutate(
        self,
        request: web.Request,
    ) -> web.Response:
        payload: dict[str, Any] = await request.json()
        uid = payload["request"]["uid"]
        admission_review = AdmissionReviewResponse(uid=uid)

        obj = payload["request"]["object"]
        kind = obj.get("kind")

        handler = self._kind_handlers.get(kind)
        if not handler:
            return admission_review.allow()

        namespace = payload["request"]["namespace"]
        LOGGER.info(f"going to mutate {kind} in a namespace {namespace}")
        return await handler(obj, namespace, admission_review)

    async def _handle_pod(
        self,
        pod: dict[str, Any],
        namespace: str,
        admission_review: AdmissionReviewResponse,
    ) -> web.Response:
        # todo: ... TBD ...
        return admission_review.allow()

    async def _handle_pvc(
        self,
        pvc: dict[str, Any],
        namespace: str,
        admission_review: AdmissionReviewResponse,
    ) -> web.Response:
        pvc_metadata = pvc["metadata"]
        pvc_labels = pvc_metadata.get("labels", {}) or {}
        pvc_annotations = pvc_metadata.get("annotations", {}) or {}
        pvc_name = pvc_metadata["name"]

        namespace_obj = await self._kube_client.get(
            self._kube_client.generate_namespace_url(namespace)
        )
        try:
            namespace_labels = namespace_obj["metadata"]["labels"]
            org = namespace_labels[APOLO_ORG_LABEL]
            project = namespace_labels[APOLO_PROJECT_LABEL]
        except KeyError:
            return admission_review.decline(
                status_code=HTTPBadRequest.status_code,
                message="Namespace lacks required org / project labels",
            )

        now = datetime_dump(utc_now())

        if "annotations" not in pvc_metadata:
            LOGGER.info("PVC doesn't define any annotation. Going to create")
            admission_review.add_patch(PATH_ANNOTATIONS, value={})

        if "labels" not in pvc_metadata:
            LOGGER.info("PVC doesn't define any label. Going to create")
            admission_review.add_patch(PATH_LABELS, value={})

        # populate necessary disk annotations
        for annotation_key, annotation_value in (
            (APOLO_DISK_API_CREATED_AT_ANNOTATION, now),
            (DISK_API_CREATED_AT_ANNOTATION, now),
        ):
            self._add_key_value_if_not_exist(
                admission_review=admission_review,
                collection=pvc_annotations,
                collection_path=PATH_ANNOTATIONS,
                key=annotation_key,
                value=annotation_value,
            )

        # populate necessary disk labels
        for label_key, label_value in (
            (DISK_API_MARK_LABEL, "true"),
            (APOLO_DISK_API_MARK_LABEL, "true"),
            (DISK_API_ORG_LABEL, org),
            (APOLO_ORG_LABEL, org),
            (DISK_API_PROJECT_LABEL, project),
            (APOLO_PROJECT_LABEL, project),
            (APOLO_USER_LABEL, project),
            (USER_LABEL, project),
        ):
            self._add_key_value_if_not_exist(
                admission_review=admission_review,
                collection=pvc_labels,
                collection_path=PATH_LABELS,
                key=label_key,
                value=label_value,
            )

        LOGGER.info(f"Will submit patch operations: {admission_review.patch}")

        await self._create_disk_naming(
            namespace=namespace,
            org=org,
            project=project,
            pvc_name=pvc_name,
            annotations=pvc_annotations,
            admission_review=admission_review,
        )

        # set a proper storage class name
        admission_review.add_patch("/spec/storageClassName", self._storage_class_name)
        return admission_review.allow()

    async def _create_disk_naming(
        self,
        namespace: str,
        org: str,
        project: str,
        pvc_name: str,
        annotations: dict[str, Any],
        admission_review: AdmissionReviewResponse,
    ) -> None:
        disk_name = annotations.get(APOLO_DISK_API_NAME_ANNOTATION) or annotations.get(
            DISK_API_NAME_ANNOTATION
        )
        if not disk_name:
            LOGGER.info("disk naming was not requested")
            return

        # a special case for a statefulset.
        # it creates a PVCs with the incremental index upfront, e.g., pvc-0, pvc-1, etc.
        # if a statefulset wants to use disk naming feature,
        # we'll need to parse such indexes and use them, to avoid name disk clashes,
        # due to a disk name uniqueness property.
        disk_name_search = re.search(RE_STATEFUL_SET_PVC_NAME_INDEX, pvc_name)
        if disk_name_search:
            disk_name_index = disk_name_search.group("index")
            # append index to the original disk name, so if a requested name was a
            # `test-disk`, the resulting name will be `test-disk-0`, `test-disk-1`, etc.
            disk_name = f"{disk_name}{disk_name_index}"

            # update disk name annotations
            for key in (APOLO_DISK_API_NAME_ANNOTATION, DISK_API_NAME_ANNOTATION):
                admission_review.add_patch(
                    path=f"{PATH_ANNOTATIONS}/{escape_json_pointer(key)}",
                    value=disk_name,
                )

        disk_name = Service.get_disk_naming_name(
            disk_name,
            org_name=org,
            project_name=project,
        )
        LOGGER.info(f"will create a disk naming {disk_name}")
        disk_naming = DiskNaming(namespace, name=disk_name, disk_id=pvc_name)

        try:
            await self._kube_client.create_disk_naming(disk_naming)
        except ResourceExists:
            existing_disk_naming = await self._kube_client.get_disk_naming(
                namespace=namespace, name=disk_name
            )
            # check whether this disk is related to this particular PVC.
            # this might be a case on an admission controller reinvocation.
            # disk name must be unique, so if it's linked to another PVC, we raise an error here.
            if existing_disk_naming.disk_id != pvc_name:
                raise DiskNameUsed(
                    f"Disk with name {disk_name} already exists for project {project}"
                )

    @staticmethod
    def _add_key_value_if_not_exist(
        admission_review: AdmissionReviewResponse,
        collection: dict[str, str],
        collection_path: str,
        key: str,
        value: str,
    ) -> None:
        """Adds a patch add op if key doesn't exist in an original collection"""
        if key in collection:
            return
        admission_review.add_patch(
            path=f"{collection_path}/{escape_json_pointer(key)}", value=value
        )


@web.middleware
async def handle_exceptions(
    request: web.Request,
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> web.StreamResponse:
    try:
        return await handler(request)
    except DiskNameUsed as e:
        req_json = await request.json()
        return AdmissionReviewResponse(uid=req_json["request"]["uid"]).decline(
            status_code=400, message=str(e)
        )
    except Exception as exc:
        err_message = "Unexpected error happened"
        LOGGER.exception("%s: %s", err_message, exc)
        req_json = await request.json()
        admission_response = AdmissionReviewResponse(uid=req_json["request"]["uid"])
        return admission_response.decline(status_code=400, message=err_message)


async def create_app(config: Config) -> web.Application:
    app = web.Application(
        middlewares=[
            handle_exceptions,
        ]
    )
    app[CONFIG_KEY] = config

    async def _init_app(app: web.Application) -> AsyncIterator[None]:
        async with AsyncExitStack() as exit_stack:
            LOGGER.info("Initializing Kube client")
            kube_client = await exit_stack.enter_async_context(
                create_kube_client(config.kube)
            )
            app[KUBE_CLIENT_KEY] = kube_client
            yield

    app.cleanup_ctx.append(_init_app)
    await AdmissionControllerHandler(app=app).register()
    return app
