import abc
from typing import List, Union, Dict, Iterable

from kgqa_signatures.logger import get_logger
from kgqa_signatures.wikidata.sparql_condition import SparqlCondition

logger = get_logger()


class AbstractService(abc.ABC):
    @abc.abstractmethod
    def get_entity_one_hop_neighbours(
        self,
        entity_id: str,
        direct_only: bool = False,
        conditions: Union[List[SparqlCondition], None] = None,
        match_all_conditions: bool = True,
        hop_count: int = 1,
    ):
        pass

    @abc.abstractmethod
    def count_matches(self, candidates: Iterable[str], conditions: List[SparqlCondition]) -> Dict[str, int]:
        pass

