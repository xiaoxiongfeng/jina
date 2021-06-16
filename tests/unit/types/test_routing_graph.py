from google.protobuf import json_format

from jina.types.routing.graph import RoutingGraph
from jina.proto import jina_pb2


def get_next_routes(routing):
    routing_pb = jina_pb2.RoutingGraphProto()
    json_format.ParseDict(routing, routing_pb)
    routing = RoutingGraph(routing_pb)
    return routing.get_next_targets()


def test_single_routing():
    graph = {
        'active_pod_index': 0,
        'pods': [
            {'host': '0.0.0.0', 'port': 1230, 'expected_parts': 1},
        ],
        'edges': {0: {'targets': []}},
    }
    next_routes = get_next_routes(graph)

    assert len(next_routes) == 0


def test_simple_routing():
    graph = {
        'active_pod_index': 0,
        'pods': [
            {'host': '0.0.0.0', 'port': 1230, 'expected_parts': 1},
            {'host': '0.0.0.0', 'port': 1231, 'expected_parts': 1},
        ],
        'edges': {0: {'targets': [1]}, 1: {'targets': []}},
    }
    next_routes = get_next_routes(graph)

    assert len(next_routes) == 1
    assert next_routes[0].active_pod_index == 1


def test_double_routing():
    graph = {
        'active_pod_index': 0,
        'pods': [
            {'host': '0.0.0.0', 'port': 1230, 'expected_parts': 0},
            {'host': '0.0.0.0', 'port': 1231, 'expected_parts': 1},
            {'host': '0.0.0.0', 'port': 1232, 'expected_parts': 1},
            {'host': '0.0.0.0', 'port': 1230, 'expected_parts': 2},
        ],
        'edges': {
            0: {'targets': [1, 2]},
            1: {'targets': [3]},
            2: {'targets': [3]},
            3: {'targets': []},
        },
    }
    next_routes = get_next_routes(graph)

    assert len(next_routes) == 2
    assert next_routes[0].active_pod_index == 1
    assert next_routes[1].active_pod_index == 2


def test_nested_routing():
    graph = {
        'active_pod_index': 0,
        'pods': [
            {'host': '0.0.0.0', 'port': 1230, 'expected_parts': 1},
            {'host': '0.0.0.0', 'port': 1231, 'expected_parts': 1},
            {'host': '0.0.0.0', 'port': 1232, 'expected_parts': 1},
            {'host': '0.0.0.0', 'port': 1233, 'expected_parts': 1},
            {'host': '0.0.0.0', 'port': 1230, 'expected_parts': 2},
        ],
        'edges': {
            0: {'targets': [1, 2]},
            1: {'targets': [3]},
            2: {'targets': [4]},
            3: {'targets': [4]},
            4: {'targets': []},
        },
    }
    next_routes = get_next_routes(graph)

    assert len(next_routes) == 2
    assert next_routes[0].active_pod_index == 1
    assert next_routes[1].active_pod_index == 2

    graph['active_pod_index'] = 1
    next_routes = get_next_routes(graph)

    assert len(next_routes) == 1
    assert next_routes[0].active_pod_index == 3

    graph['active_pod_index'] = 2
    next_routes = get_next_routes(graph)

    assert len(next_routes) == 1
    assert next_routes[0].active_pod_index == 4

    graph['active_pod_index'] = 3
    next_routes = get_next_routes(graph)

    assert len(next_routes) == 1
    assert next_routes[0].active_pod_index == 4

    graph['active_pod_index'] = 4
    next_routes = get_next_routes(graph)

    assert len(next_routes) == 0


def test_topological_sorting():
    graph = {
        'active_pod_index': 0,
        'pods': [
            {'host': '0.0.0.0', 'port': 1230, 'expected_parts': 1},
            {'host': '0.0.0.0', 'port': 1231, 'expected_parts': 1},
            {'host': '0.0.0.0', 'port': 1232, 'expected_parts': 1},
            {'host': '0.0.0.0', 'port': 1233, 'expected_parts': 1},
            {'host': '0.0.0.0', 'port': 1230, 'expected_parts': 2},
        ],
        'edges': {
            0: {'targets': [1, 2]},
            1: {'targets': [3]},
            2: {'targets': [4]},
            3: {'targets': [4]},
            4: {'targets': []},
        },
    }
    routing_pb = jina_pb2.RoutingGraphProto()
    json_format.ParseDict(graph, routing_pb)
    routing = RoutingGraph(routing_pb)
    topological_sorting = routing._topological_sort()

    assert topological_sorting[0] == 0
    assert topological_sorting[1] in [1, 2]
    assert topological_sorting[2] in [1, 2, 3]
    assert topological_sorting[3] in [2, 3]
    assert topological_sorting[4] == 4
    assert topological_sorting == [0, 2, 1, 3, 4]


def test_cycle():
    graph = {
        'active_pod_index': 0,
        'pods': [
            {'host': '0.0.0.0', 'port': 1230, 'expected_parts': 1},
            {'host': '0.0.0.0', 'port': 1231, 'expected_parts': 1},
        ],
        'edges': {0: {'targets': [1]}, 1: {'targets': [0]}},
    }
    routing_pb = jina_pb2.RoutingGraphProto()
    json_format.ParseDict(graph, routing_pb)
    routing = RoutingGraph(routing_pb)
    assert not routing.is_acyclic()


def test_no_cycle():
    graph = {
        'active_pod_index': 0,
        'pods': [
            {'host': '0.0.0.0', 'port': 1230, 'expected_parts': 1},
            {'host': '0.0.0.0', 'port': 1231, 'expected_parts': 1},
            {'host': '0.0.0.0', 'port': 1232, 'expected_parts': 1},
            {'host': '0.0.0.0', 'port': 1233, 'expected_parts': 1},
            {'host': '0.0.0.0', 'port': 1234, 'expected_parts': 1},
        ],
        'edges': {
            2: {'targets': [1]},
            1: {'targets': [0]},
            0: {'targets': [3]},
            3: {'targets': [4]},
            4: {'targets': []},
        },
    }
    routing_pb = jina_pb2.RoutingGraphProto()
    json_format.ParseDict(graph, routing_pb)
    routing = RoutingGraph(routing_pb)
    assert routing.is_acyclic()
