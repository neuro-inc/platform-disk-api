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
from uuid import uuid4

from aiohttp import web
from aiohttp.web_exceptions import (
    HTTPBadRequest,
    HTTPConflict,
    HTTPNotFound,
    HTTPForbidden,
    HTTPUnprocessableEntity,
)
from apolo_kube_client.errors import ResourceExists

from .schema import InjectionSchema, MountMode
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
    DiskNotFound,
    DiskConflict,
    DiskServiceError,
    DiskAlreadyInUse,
)
from ..utils import datetime_dump, utc_now

LOGGER = logging.getLogger(__name__)

ANNOTATION_APOLO_INJECT_DISK = "platform.apolo.us/inject-disk"

LABEL_APOLO_ORG_NAME = "platform.apolo.us/org"
LABEL_APOLO_PROJECT_NAME = "platform.apolo.us/project"

INJECTED_VOLUME_NAME_PREFIX = "disk-auto-injected-volume"


PATH_ANNOTATIONS = "/metadata/annotations"
PATH_LABELS = "/metadata/labels"

# endswith dash and number, e.g.: -0, -1, -2, etc
RE_STATEFUL_SET_PVC_NAME_INDEX = re.compile(r"(?P<index>-\d+$)")

KUBE_CLIENT_KEY = web.AppKey("kube_client", KubeClient)
CONFIG_KEY = web.AppKey("config", Config)


ERROR_TO_HTTP_CODE = {
    DiskServiceError: HTTPNotFound.status_code,
    DiskNotFound: HTTPNotFound.status_code,
    DiskConflict: HTTPConflict.status_code,
    DiskNameUsed: HTTPConflict.status_code,
    DiskAlreadyInUse: HTTPConflict.status_code,
}


class AdmissionControllerApiError(Exception):
    def __init__(
        self,
        message: str,
        status_code: int,
    ):
        self.message = message
        self.status_code = status_code


def create_injection_volume_name() -> str:
    """Creates a random volume name"""
    return f"{INJECTED_VOLUME_NAME_PREFIX}-{str(uuid4())[:8]}"


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
        LOGGER.info("allowing mutation")
        return web.json_response(self._to_primitive(allowed=True))

    def decline(self, status_code: int, message: str) -> web.Response:
        LOGGER.info("declining mutation")
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
        self._disk_service: Service
        self._app = app
        self._storage_class_name: str | None = self._config.disk.k8s_storage_class
        self._kind_handlers: dict[
            str,
            Callable[
                [dict[str, Any], str, str, str, AdmissionReviewResponse],
                Awaitable[web.Response],
            ],
        ] = {
            "Pod": self._handle_pod,
            "PersistentVolumeClaim": self._handle_pvc,
        }

    def register(self) -> None:
        self._app.add_routes(
            [
                web.get("/ping", self.handle_ping),
                web.post("/mutate", self.handle_post_mutate),
            ]
        )

    async def init(self) -> None:
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
        self._disk_service = Service(
            kube_client=self._kube_client, storage_class_name=self._storage_class_name
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
        LOGGER.info(f"trying to mutate {kind} in a namespace {namespace}")
        namespace_org, namespace_project = await self._get_namespace_org_project(
            namespace
        )
        return await handler(
            obj,
            namespace,
            namespace_org,
            namespace_project,
            admission_review,
        )

    async def _handle_pod(
        self,
        pod: dict[str, Any],
        namespace: str,
        namespace_org: str,
        namespace_project: str,
        admission_review: AdmissionReviewResponse,
    ) -> web.Response:
        spec = pod["spec"]
        containers = spec.get("containers") or []
        if not containers:
            LOGGER.info("POD won't be mutated because doesnt define containers")
            return admission_review.allow()

        pod_metadata = pod["metadata"]
        pod_annotations = pod_metadata.get("annotations", {}) or {}

        if ANNOTATION_APOLO_INJECT_DISK not in pod_annotations:
            return admission_review.allow()

        pod_labels = pod_metadata.get("labels", {}) or {}

        # check for org/proj labels on the pod.
        # allow if there is no such to give a chance for metadata injector to reinvoke
        if LABEL_APOLO_ORG_NAME not in pod_labels:
            LOGGER.info("Pod is not ready, missing label %s", LABEL_APOLO_ORG_NAME)
            return admission_review.allow()
        if LABEL_APOLO_PROJECT_NAME not in pod_labels:
            LOGGER.info("Pod is not ready, missing label %s", LABEL_APOLO_PROJECT_NAME)
            return admission_review.allow()

        raw_injection_spec = pod_annotations[ANNOTATION_APOLO_INJECT_DISK]

        try:
            injection_spec = InjectionSchema.validate_json(raw_injection_spec)
        except Exception as e:
            error_message = "injection spec is invalid"
            LOGGER.exception(error_message)
            raise AdmissionControllerApiError(
                message=error_message, status_code=HTTPUnprocessableEntity.status_code
            ) from e

        # ensure disk URIs, namespace labels and the pod labels are same in terms of org/project values
        for injection_schema in injection_spec:
            for comparable in (
                {injection_schema.org, namespace_org, pod_labels[APOLO_ORG_LABEL]},
                {
                    injection_schema.project,
                    namespace_project,
                    pod_labels[APOLO_PROJECT_LABEL],
                },
            ):
                if len(comparable) != 1:
                    error_message = "metadata value mismatch"
                    LOGGER.error("%s: %s", error_message, comparable)
                    raise AdmissionControllerApiError(
                        message=error_message,
                        status_code=HTTPForbidden.status_code,
                    )

        LOGGER.info("All checks passed. Going to inject disks")

        # ensure POD has volumes
        self._add_key_value_if_not_exist(
            admission_review=admission_review,
            collection=spec,
            collection_path="/spec",
            key="volumes",
            value=[],
        )

        # add claims
        for injection_schema in injection_spec:
            future_volume_name = create_injection_volume_name()
            disk = await self._disk_service.resolve_disk(
                disk_id_or_name=injection_schema.disk_id_or_name,
                org_name=injection_schema.org,
                project_name=injection_schema.project,
            )

            admission_review.add_patch(
                path="/spec/volumes/-",
                value={
                    "name": future_volume_name,
                    "persistentVolumeClaim": {
                        "claimName": disk.id,
                    },
                },
            )

            # add a volumeMount with mount path for all the POD containers
            for container_idx in range(len(containers)):
                patch_value: dict[str, str | bool] = {
                    "name": future_volume_name,
                    "mountPath": injection_schema.mount_path,
                }
                if injection_schema.mount_mode is MountMode.READ_ONLY:
                    patch_value["readOnly"] = True

                admission_review.add_patch(
                    path=f"/spec/containers/{container_idx}/volumeMounts/-",
                    value=patch_value,
                )

        return admission_review.allow()

    async def _handle_pvc(
        self,
        pvc: dict[str, Any],
        namespace: str,
        namespace_org: str,
        namespace_project: str,
        admission_review: AdmissionReviewResponse,
    ) -> web.Response:
        pvc_metadata = pvc["metadata"]
        pvc_labels = pvc_metadata.get("labels", {}) or {}
        pvc_annotations = pvc_metadata.get("annotations", {}) or {}
        pvc_name = pvc_metadata["name"]

        now = datetime_dump(utc_now())

        for key in ("labels", "annotations"):
            self._add_key_value_if_not_exist(
                admission_review,
                collection=pvc_metadata,
                collection_path="/metadata",
                key=key,
                value={},
            )

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
            (DISK_API_ORG_LABEL, namespace_org),
            (APOLO_ORG_LABEL, namespace_org),
            (DISK_API_PROJECT_LABEL, namespace_project),
            (APOLO_PROJECT_LABEL, namespace_project),
            (APOLO_USER_LABEL, namespace_project),
            (USER_LABEL, namespace_project),
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
            org=namespace_org,
            project=namespace_project,
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
        # we'll need to parse such indexes and use them, to avoid name clashes,
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
        value: Any,
    ) -> None:
        """Adds a patch add op if key doesn't exist in an original collection"""
        if key in collection:
            return
        admission_review.add_patch(
            path=f"{collection_path}/{escape_json_pointer(key)}", value=value
        )

    async def _get_namespace_org_project(self, namespace: str) -> tuple[str, str]:
        namespace_obj = await self._kube_client.get(
            self._kube_client.generate_namespace_url(namespace)
        )
        try:
            namespace_labels = namespace_obj["metadata"]["labels"]
            org = namespace_labels[APOLO_ORG_LABEL]
            project = namespace_labels[APOLO_PROJECT_LABEL]
        except KeyError:
            raise AdmissionControllerApiError(
                status_code=HTTPBadRequest.status_code,
                message="Namespace lacks required org / project labels",
            )
        return org, project


@web.middleware
async def handle_exceptions(
    request: web.Request,
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> web.StreamResponse:
    try:
        return await handler(request)
    except DiskServiceError as e:
        req_json = await request.json()
        uid = req_json["request"]["uid"]
        status_code = ERROR_TO_HTTP_CODE.get(type(e), HTTPBadRequest.status_code)
        return AdmissionReviewResponse(uid=uid).decline(
            status_code=status_code, message=str(e)
        )
    except AdmissionControllerApiError as e:
        req_json = await request.json()
        uid = req_json["request"]["uid"]
        return AdmissionReviewResponse(uid=uid).decline(
            status_code=e.status_code, message=e.message
        )
    except Exception as exc:
        err_message = "Unexpected error happened"
        LOGGER.exception("%s: %s", err_message, exc)
        req_json = await request.json()
        admission_response = AdmissionReviewResponse(uid=req_json["request"]["uid"])
        return admission_response.decline(
            status_code=HTTPBadRequest.status_code, message=err_message
        )


async def create_app(config: Config) -> web.Application:
    app = web.Application(
        middlewares=[
            handle_exceptions,
        ]
    )
    app[CONFIG_KEY] = config
    handler = AdmissionControllerHandler(app=app)
    handler.register()

    async def _init_app(app: web.Application) -> AsyncIterator[None]:
        async with AsyncExitStack() as exit_stack:
            LOGGER.info("Initializing Kube client")
            kube_client = await exit_stack.enter_async_context(
                create_kube_client(config.kube)
            )
            app[KUBE_CLIENT_KEY] = kube_client
            await handler.init()
            yield

    app.cleanup_ctx.append(_init_app)
    return app
