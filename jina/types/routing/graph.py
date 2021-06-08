from ...proto import jina_pb2


class RoutingGraph:
    def __init__(
        self,
        graph: 'jina_pb2.RoutingGraphProto' = None,
        *args,
        **kwargs,
    ):
        self._graph = graph

    @property
    def active_pod(self):
        return self._graph.active_pod

    @property
    def pods(self):
        return self._graph.pods

    @property
    def active_out_edges(self):
        return self.pods[self.active_pod].out_edges

    def get_next_targets(self):
        targets = []
        for next_pod in self.active_out_edges:

            new_graph = jina_pb2.RoutingGraphProto()
            new_graph.CopyFrom(self._graph)
            new_graph.active_pod = next_pod
            targets.append(new_graph)
        return targets
