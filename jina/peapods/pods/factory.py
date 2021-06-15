from argparse import Namespace
from typing import Optional, Set

from .compound import CompoundPod
from .. import BasePod
from .. import Pod
from ...enums import SocketType


class PodFactory:
    """
    A PodFactory is a factory class, abstracting the Pod creation
    """

    @staticmethod
    def build_pod(args: 'Namespace', needs: Optional[Set[str]] = None) -> BasePod:
        """Build an implementation of a `BasePod` interface

        :param args: pod arguments parsed from the CLI.
        :param needs: pod names of preceding pods

        :return: the created BasePod
        """
        if getattr(args, 'replicas', 1) > 1:
            pod = CompoundPod(args, needs=needs)
        else:
            pod = Pod(args, needs=needs)
        pod.head_args.socket_in = SocketType.ROUTER_BIND
        pod.tail_args.dynamic_out_routing = True
        pod.tail_args.socket_out = SocketType.DEALER_CONNECT
        return pod
