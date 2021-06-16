from ...proto import jina_pb2


class TargetPod:
    def __init__(self, target: 'jina_pb2.TargetPodProto'):
        self.proto = target

    @property
    def port(self):
        return self.proto.port

    @property
    def host(self):
        return self.proto.host

    @property
    def full_address(self):
        return f'{self.host}:{self.port}'

    @property
    def expected_parts(self):
        return self.proto.expected_parts


class RoutingGraph:
    def __init__(self, graph: 'jina_pb2.RoutingGraphProto' = None):
        self.proto = graph

    @property
    def edges(self):
        return self.proto.edges

    def get_out_edges(self, pod_id):
        return self.proto.edges[pod_id].targets

    @property
    def active_pod_index(self):
        return self.proto.active_pod_index

    @property
    def active_pod(self):
        return TargetPod(self.pods[self.active_pod_index])

    @property
    def pods(self):
        return self.proto.pods

    def get_next_targets(self):
        targets = []
        for next_pod_index in self.get_out_edges(self.active_pod_index):
            new_graph = jina_pb2.RoutingGraphProto()
            new_graph.CopyFrom(self.proto)
            new_graph.active_pod_index = next_pod_index
            targets.append(RoutingGraph(new_graph))
        return targets

    def is_acyclic(self):
        topological_sorting = self._topological_sort()
        position_lookup = {
            index: position for position, index in enumerate(topological_sorting)
        }

        for first in topological_sorting:
            for second in self.get_out_edges(first):

                if position_lookup[first] > position_lookup[second]:

                    return False
        return True

    def get_topological_sorted_pods(self):
        topological_order = self._topological_sort()
        return [self.pods[pod_id] for pod_id in topological_order]

    def _topological_sort(self):
        """
        Calculates a topological sorting. It uses internally _topological_sort_pod()
        For more information see https://en.wikipedia.org/wiki/Topological_sorting
        """

        num_pods = len(self.pods)
        visited = [False] * num_pods
        stack = []

        for pod_index in range(num_pods):
            if not visited[pod_index]:
                self._topological_sort_pod(pod_index, visited, stack)

        return stack[::-1]

    def _topological_sort_pod(self, pod_index, visited, stack):
        visited[pod_index] = True

        for i in self.get_out_edges(pod_index):
            if not visited[i]:
                self._topological_sort_pod(i, visited, stack)

        stack.append(pod_index)
