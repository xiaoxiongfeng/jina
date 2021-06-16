import pytest

from jina import Flow, __default_host__
from jina.enums import SocketType
from jina.helper import get_internal_ip, get_public_ip


def ip_from(flow, pod_number):
    return flow['gateway'].args.routing_graph.pods[pod_number].host


@pytest.mark.parametrize(
    'local_ip, on_public', [(get_internal_ip(), False), (get_public_ip(), True)]
)
def test_remote_pod_local_gateway(local_ip, on_public):
    # BIND socket's host must always be 0.0.0.0
    remote_ip = '111.111.111.111'
    f = Flow(expose_public=on_public).add(host=remote_ip)
    f.build()
    assert ip_from(f, 0) == __default_host__
    assert ip_from(f, 1) == remote_ip
    assert ip_from(f, -1) == ip_from(f, 0)


@pytest.mark.parametrize(
    'local_ip, on_public', [(get_internal_ip(), False), (get_public_ip(), True)]
)
def test_remote_pod_local_pod_local_gateway_input_socket_pull_connect_from_remote(
    local_ip, on_public
):
    remote_ip = '111.111.111.111'
    f = Flow(expose_public=on_public).add(host=remote_ip).add().build()

    assert ip_from(f, 0) == __default_host__
    assert ip_from(f, 1) == remote_ip
    assert ip_from(f, 2) == __default_host__
    assert ip_from(f, -1) == ip_from(f, 0)


@pytest.mark.parametrize(
    'local_ip, on_public', [(get_internal_ip(), False), (get_public_ip(), True)]
)
def test_remote_pod_local_pod_local_gateway(local_ip, on_public):
    remote_ip = '111.111.111.111'
    f = Flow(expose_public=on_public).add(host=remote_ip).add().build()
    assert ip_from(f, 0) == __default_host__
    assert ip_from(f, 1) == remote_ip
    assert ip_from(f, 2) == __default_host__
    assert ip_from(f, -1) == ip_from(f, 0)


@pytest.mark.parametrize(
    'local_ip, on_public', [(get_internal_ip(), False), (get_public_ip(), True)]
)
def test_remote_pod_local_pod_remote_pod_local_gateway_input_socket_pull_connect_from_remote(
    local_ip, on_public
):
    remote1 = '111.111.111.111'
    remote2 = '222.222.222.222'

    f = Flow(expose_public=on_public).add(host=remote1).add().add(host=remote2).build()
    assert ip_from(f, 0) == __default_host__
    assert ip_from(f, 1) == remote1
    assert ip_from(f, 2) == __default_host__
    assert ip_from(f, 3) == remote2
    assert ip_from(f, -1) == ip_from(f, 0)


@pytest.mark.parametrize(
    'local_ip, on_public', [(get_internal_ip(), False), (get_public_ip(), True)]
)
def test_local_pod_remote_pod_remote_pod_local_gateway(local_ip, on_public):
    remote1 = '111.111.111.111'
    remote2 = '222.222.222.222'

    f = Flow(expose_public=on_public).add().add(host=remote1).add(host=remote2)
    f.build()
    assert ip_from(f, 0) == __default_host__
    assert ip_from(f, 1) == __default_host__
    assert ip_from(f, 2) == remote1
    assert ip_from(f, 3) == remote2
    assert ip_from(f, -1) == ip_from(f, 0)


def test_gateway_remote():
    remote1 = '111.111.111.111'
    f = Flow().add(host=remote1).build()

    assert f['pod0'].args.socket_in.is_bind
    assert not f['pod0'].args.socket_out.is_bind


def test_gateway_remote_local():
    """

    remote  IN: 0.0.0.0:61913 (PULL_BIND)   internal_ip:61914 (PUSH_CONNECT)
    pod1    IN: 0.0.0.0:61914 (PULL_BIND)    0.0.0.0:61918 (PUSH_BIND)
    gateway IN: 0.0.0.0:61918 (PULL_CONNECT)  111.111.111.111:61913 (PUSH_CONNECT)

    :return:
    """
    remote1 = '111.111.111.111'
    f = Flow().add(host=remote1).add().build()

    assert f['pod0'].args.socket_in == SocketType.ROUTER_BIND
    assert f['pod0'].args.socket_out == SocketType.DEALER_CONNECT

    assert f['pod1'].args.socket_in == SocketType.ROUTER_BIND
    assert f['pod1'].args.socket_out == SocketType.DEALER_CONNECT
    assert f['gateway'].args.socket_in == SocketType.ROUTER_BIND
    assert f['gateway'].args.socket_out == SocketType.DEALER_CONNECT


def test_gateway_local_remote():
    """

    pod0    IN: 0.0.0.0:62322 (PULL_BIND)   3.135.17.36:62326 (PUSH_CONNECT)
    remote  IN: 0.0.0.0:62326 (PULL_BIND)   0.0.0.0:62327 (PUSH_BIND)
    gateway IN: 3.135.17.36:62327 (PULL_CONNECT)    0.0.0.0:62322 (PUSH_CONNECT)

    :return:
    """
    remote1 = '111.111.111.111'
    f = Flow().add().add(host=remote1).build()

    assert f['pod0'].args.socket_in.is_bind
    assert not f['pod0'].args.socket_out.is_bind
    assert f['pod1'].args.socket_in.is_bind
    assert not f['pod1'].args.socket_out.is_bind
