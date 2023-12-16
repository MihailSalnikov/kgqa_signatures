from typing import List, Union, Dict, Iterable
import igraph

from kgqa_signatures.logger import get_logger
from kgqa_signatures.wikidata.abstract_service import AbstractService
from kgqa_signatures.wikidata.sparql_condition import SparqlCondition

logger = get_logger()


class IGraphService(AbstractService):

    def __init__(self, igraph_object):
        self.G = igraph_object

    def get_entity_one_hop_neighbours(
            self,
            entity_id: str,
            direct_only: bool = False,
            conditions: Union[List[SparqlCondition], None] = None,
            match_all_conditions: bool = True,
            hop_count: int = 1,
    ):
        try:
            entity_index = self.G.vs.find(name=entity_id[1:]).index
        except ValueError:
            print(f"no such node: {entity_id}")
            return

        # prepare conditions
        conditions_weight_to_index = None
        if conditions is not None:
            conditions_weight_to_index = {}
            for condition in conditions:
                target_index = self.G.vs.find(name=condition.destination[1:]).index
                conditions_weight_to_index[int(condition.connection[1:])] = target_index

        # process all edges of entity_id
        parsed_result = []
        for entity_edge_index in self.G.incident(entity_index):
            entity_edge = self.G.es[entity_edge_index]

            # skip system properties like label
            if int(entity_edge['weight']) <= 0:
                continue

            # check direct only
            if entity_edge.source != entity_index and direct_only:
                continue

            # understand with whom we are connected bu this edge
            neighbour_index = entity_edge.source if entity_edge.source != entity_index else entity_edge.target

            # check conditions
            if conditions_weight_to_index is not None:
                neighbour_edges = self.G.incident(neighbour_index)
                neighbour_edges = list(map(lambda x: self.G.es[x], neighbour_edges))
                match_conditions = 0
                for edge in neighbour_edges:
                    if edge["weight"] in conditions_weight_to_index and conditions_weight_to_index[int(edge["weight"])] in (edge.source, edge.target):
                        match_conditions += 1
                        if not match_all_conditions:
                            break

                if match_conditions == 0 or (match_conditions != len(conditions) and match_all_conditions):
                    continue

            parsed_result.append((f"P{int(entity_edge['weight'])}", f"Q{self.G.vs[neighbour_index]['name']}"))

        return parsed_result

    def count_matches(self, candidates: Iterable[str], conditions: List[SparqlCondition]) -> Dict[str, int]:
        # prepare conditions
        conditions_weight_to_index = None
        if conditions is not None:
            conditions_weight_to_index = {}
            for condition in conditions:
                target_index = self.G.vs.find(name=condition.destination[1:]).index
                conditions_weight_to_index[int(condition.connection[1:])] = target_index

        result = {}
        for candidate in candidates:
            try:
                candidate_index = self.G.vs.find(name=candidate[1:]).index
            except ValueError:
                print(f"no such node: {candidate}")
                result[candidate] = 0
                continue

            candidate_edges = self.G.incident(candidate_index)
            candidate_edges = list(map(lambda x: self.G.es[x], candidate_edges))
            match_conditions = 0
            for edge in candidate_edges:
                if edge["weight"] in conditions_weight_to_index and conditions_weight_to_index[int(edge["weight"])] in (edge.source, edge.target):
                    match_conditions += 1

            result[candidate] = match_conditions

        return result


if __name__ == "__main__":
    G = igraph.Graph.Read(
        "../wikidata_triples1.txt",
        names=True,
        format="lgl",
        directed=True,
        weights=True,
    )
    service = IGraphService(G)
    neighbours = service.get_entity_one_hop_neighbours("Q61")
    print(neighbours)
