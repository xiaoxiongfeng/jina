from google.protobuf import json_format

from jina.types.routing.graph import RoutingGraph
from jina.proto import jina_pb2


def get_next_routes(routing):
    routing_pb = jina_pb2.RoutingGraphProto()
    json_format.ParseDict(routing, routing_pb)
    routing = RoutingGraph(routing_pb)
    return routing.get_next_targets()


def test_single_routing():
    table = {
        'active_pod_index': 0,
        'pods': [
            {'pod_address': '0', 'expected_parts': 1, 'out_edges': []},
        ],
    }
    next_routes = get_next_routes(table)

    assert len(next_routes) == 0


def test_simple_routing():
    table = {
        'active_pod_index': 0,
        'pods': [
            {'pod_address': '0', 'expected_parts': 1, 'out_edges': [1]},
            {'pod_address': '1', 'expected_parts': 1, 'out_edges': []},
        ],
    }
    next_routes = get_next_routes(table)

    assert len(next_routes) == 1
    assert next_routes[0].active_pod_index == 1


def test_double_routing():
    table = {
        'active_pod_index': 0,
        'pods': [
            {'pod_address': '0', 'expected_parts': 1, 'out_edges': [1, 2]},
            {'pod_address': '1', 'expected_parts': 1, 'out_edges': [3]},
            {'pod_address': '2', 'expected_parts': 1, 'out_edges': [3]},
            {'pod_address': '3', 'expected_parts': 2, 'out_edges': []},
        ],
    }
    next_routes = get_next_routes(table)

    assert len(next_routes) == 2
    assert next_routes[0].active_pod_index == 1
    assert next_routes[1].active_pod_index == 2


def test_nested_routing():
    table = {
        'active_pod_index': 0,
        'pods': [
            {'pod_address': '0', 'expected_parts': 1, 'out_edges': [1, 2]},
            {'pod_address': '1', 'expected_parts': 1, 'out_edges': [3]},
            {'pod_address': '2', 'expected_parts': 1, 'out_edges': [4]},
            {'pod_address': '3', 'expected_parts': 1, 'out_edges': [4]},
            {'pod_address': '4', 'expected_parts': 2, 'out_edges': []},
        ],
    }
    next_routes = get_next_routes(table)

    assert len(next_routes) == 2
    assert next_routes[0].active_pod_index == 1
    assert next_routes[1].active_pod_index == 2

    table['active_pod_index'] = 1
    next_routes = get_next_routes(table)

    assert len(next_routes) == 1
    assert next_routes[0].active_pod_index == 3

    table['active_pod_index'] = 2
    next_routes = get_next_routes(table)

    assert len(next_routes) == 1
    assert next_routes[0].active_pod_index == 4

    table['active_pod_index'] = 3
    next_routes = get_next_routes(table)

    assert len(next_routes) == 1
    assert next_routes[0].active_pod_index == 4

    table['active_pod_index'] = 4
    next_routes = get_next_routes(table)

    assert len(next_routes) == 0
