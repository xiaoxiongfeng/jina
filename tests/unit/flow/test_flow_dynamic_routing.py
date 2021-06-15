from jina import Document, Executor, Flow, requests
from jina.logging.profile import TimeContext


class MyExecutor(Executor):
    @requests
    def add_text(self, docs, **kwargs):
        docs[0].text = 'Hello World!'


def test_simple_routing():
    f = Flow().add(uses=MyExecutor)
    with f:
        results = f.post(on='/index', inputs=[Document()], return_results=True)
        assert results[0].docs[0].text == 'Hello World!'


class MergeExecutor(Executor):
    @requests
    def add_text(self, docs, docs_matrix, **kwargs):
        if {docs[0].text, docs[1].text} == {'Hello World!', '1'}:
            docs[0].text = str(len(docs_matrix))


def test_expected_messages_routing():
    f = (
        Flow()
        .add(name='foo', uses=MyExecutor)
        .add(name='bar', uses=MergeExecutor, needs=['foo', 'gateway'])
    )

    with f:
        results = f.post(on='/index', inputs=[Document(text='1')], return_results=True)
        assert results[0].docs[0].text == '2'


def test_routing_speed():
    f = Flow().add(replicas=2).add().add()
    with f:
        with TimeContext('blabla'):
            for i in range(100):
                f.post(on='/index', inputs=[Document(text='1')], return_results=True)
