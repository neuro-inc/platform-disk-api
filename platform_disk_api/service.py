import logging

from .kube_client import KubeClient


logger = logging.getLogger()


class Service:
    def __init__(self, kube_client: KubeClient) -> None:
        self._kube_client = kube_client
