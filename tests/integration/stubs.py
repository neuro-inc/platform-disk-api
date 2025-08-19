from __future__ import annotations
import os
import tempfile
import textwrap
from collections.abc import AsyncIterator
import aiodocker
import pytest

DOCKER_NETWORK = "it_net"
NGINX_IMAGE = "nginx:1.25-alpine"


async def _ensure_image(docker: aiodocker.Docker, image: str) -> None:
    try:
        await docker.images.inspect(image)
    except aiodocker.exceptions.DockerError:
        await docker.images.pull(image)


def _write_nginx_conf() -> tuple[str, str]:
    conf = textwrap.dedent(
        r"""
        events {}
        http {
          server {
            listen 80;

            # Health
            location = /ping {
              default_type application/json;
              return 200 '{"status":"ok"}';
            }

            location = /api/v1/users/public/permissions {
                default_type application/json;
                return 200 '[]';
            }

            # What the admin calls during bootstrap
            location ~ ^/api/v1/users/.*/permissions$ {
                if ($request_method = POST) {
                    return 201;
                }
                if ($request_method = PUT) {
                    return 204;
                }
            }

            location ~ ^/api/v1/users/.*/permissions/check$ {
                default_type application/json;
                return 200 '{"missing": []}';
            }

            location ~ ^/api/v1/notifications/.*$ {
              return 201;
            }

            location ~ ^/api/v1/users/[^/]+/token$ {
              default_type application/json;
              return 200 '{"access_token":"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.mock"}';
            }

            location ~ ^/api/v1/users/(.+)$ {
              default_type application/json;
              set $user_name $1;
              return 201 '{"name":"$user_name"}';
            }

            location = /api/v1/clusters {
              default_type application/json;
              return 200 '{
                "name": "test-cluster",
                "status": "ready",
                "platform_infra_image_tag": "v1.0.0",
                "orchestrator": null,
                "storage": null,
                "registry": null,
                "monitoring": null,
                "secrets": null,
                "metrics": null,
                "disks": null,
                "buckets": null,
                "ingress": null,
                "dns": null,
                "cloud_provider": null,
                "credentials": null,
                "created_at": "2025-08-19T00:00:00+00:00",
                "timezone": null,
                "energy": null,
                "apps": null
              }';
            }

            # Fallback for everything else
            location / {
              default_type application/json;
              return 200 '{}';
            }
          }
        }
        """
    )
    tmpdir = tempfile.mkdtemp(prefix="stub-nginx-")
    conf_path = os.path.join(tmpdir, "nginx.conf")
    with open(conf_path, "w") as f:
        f.write(conf)
    return tmpdir, conf_path


async def _run_nginx_stub(
    docker: aiodocker.Docker, name: str, network: str
) -> aiodocker.containers.DockerContainer:
    await _ensure_image(docker, NGINX_IMAGE)
    host_dir, conf_path = _write_nginx_conf()

    cfg = {
        "Image": NGINX_IMAGE,
        "name": name,
        "HostConfig": {
            "NetworkMode": network,
            "Binds": [f"{host_dir}:/etc/nginx:ro"],  # mounts nginx.conf we wrote
        },
        "NetworkingConfig": {"EndpointsConfig": {network: {}}},
    }
    c = await docker.containers.create_or_replace(name=name, config=cfg)
    await c.start()
    return c


@pytest.fixture(scope="session")
async def docker_smart_stubs(
    docker: aiodocker.Docker, docker_network: str
) -> AsyncIterator[dict]:
    auth = await _run_nginx_stub(docker, "stub-auth", docker_network)
    conf = await _run_nginx_stub(docker, "stub-config", docker_network)
    notif = await _run_nginx_stub(docker, "stub-notifications", docker_network)
    try:
        yield {
            "auth_url": "http://stub-auth:80",
            "config_url": "http://stub-config:80",
            "notif_url": "http://stub-notifications:80",
        }
    finally:
        for c in (auth, conf, notif):
            for op in (c.kill,):
                try:
                    await op()
                except Exception:
                    pass
            try:
                await c.delete(force=True)
            except Exception:
                pass
