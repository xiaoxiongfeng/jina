from ...proto import jina_pb2


class RoutingGraph:
    def __init__(
        self,
        graph: 'jina_pb2.RoutingGraphProto' = None,
        *args,
        **kwargs,
    ):
        self.proto = graph

    @property
    def active_pod(self):
        return self.pods[self.active_pod_index]

    @property
    def active_pod_index(self):
        return self.proto.active_pod_index

    @property
    def pods(self):
        return self.proto.pods

    def get_next_targets(self):
        targets = []
        for next_pod in self.active_pod.out_edges:
            new_graph = jina_pb2.RoutingGraphProto()
            new_graph.CopyFrom(self.proto)
            new_graph.active_pod_index = next_pod
            targets.append(RoutingGraph(new_graph))
        return targets
