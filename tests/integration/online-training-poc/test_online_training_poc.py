import os
import time

import pytest

import numpy as np

from typing import Sequence, Dict, Optional

from jina.executors import BaseExecutor
from jina.executors.rankers import Match2DocRanker
from jina.drivers import BaseExecutableDriver
from jina.flow import Flow
from jina import Document

cur_dir = os.path.dirname(os.path.abspath(__file__))


class TrainDriver(BaseExecutableDriver):
    def __init__(self, *args, **kwargs):
        super().__init__(method='train', *args, **kwargs)

    def __call__(self, *args, **kwargs) -> None:
        self.exec_fn()


# it only inherits BaseExecutor to guarantee is discovered by jina
class RankerTrainer(BaseExecutor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.current_output = 0

    def train(self, *args, **kwargs):
        self.logger.warning('f I am being trained')
        time.sleep(1)
        self.current_output += 1


class TrainableRanker(Match2DocRanker):
    def __init__(
        self,
        score_output: float,
        trainer: Optional[RankerTrainer] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.trainer = trainer
        self.score_output = score_output
        self.match_required_keys = ['text']
        self.query_required_keys = ['text']

    def train(self, *args, **kwargs) -> None:
        self.trainer.train()

    def score(
        self,
        old_matches_scores: Sequence[Sequence[float]],
        queries_metas: Sequence[Dict],
        matches_metas: Sequence[Sequence[Dict]],
    ) -> Sequence[Sequence[float]]:
        ret = []
        for queries, matches in zip(queries_metas, matches_metas):
            r = []
            if matches:
                for _ in matches:
                    r.append(self.score_output)
            else:
                r.append(self.score_output)
            ret.append(r)
        return ret


@pytest.fixture
def tmp_workspace(tmpdir):
    os.environ['JINA_ONLINE_TRAINING_POC'] = str(tmpdir)
    yield
    del os.environ['JINA_ONLINE_TRAINING_POC']


def test_poc_online_training(tmp_workspace):
    index_docs = [
        Document(text=f'text-{i}', embedding=np.array([i] * 5)) for i in range(100)
    ]
    with Flow.load_config(os.path.join(cur_dir, 'flow-index.yml')) as f:
        f.index(inputs=index_docs)

    with Flow.load_config(os.path.join(cur_dir, 'flow-query.yml')) as f:
        f.search(inputs=index_docs[0:1], top_k=2)
        f.train(inputs=index_docs[0:1], top_k=2)
        f.train(inputs=index_docs[0:1], top_k=2)
