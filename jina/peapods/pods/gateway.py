from .. import Pod
from ...types.routing.graph import RoutingGraph


class GatewayPod(Pod):
    """A GatewayPod is a wrapper around the gateway pea.

    :param args: pod arguments parsed from the CLI. These arguments will be used for each of the replicas
    :param needs: pod names of preceding pods, the output of these pods are going into the input of this pod
    """

    def set_routing_graph(self, routing_graph: RoutingGraph):
        self.args.routing_graph = routing_graph
