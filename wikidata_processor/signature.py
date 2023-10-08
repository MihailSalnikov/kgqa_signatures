from collections import OrderedDict
from typing import Dict

from joblib import Parallel, delayed

from wikidata_processor.wikidata.service import get_entity_one_hop_neighbours
from wikidata_processor.wikidata.sparql_condition import SparqlCondition


def __get_connections_of_entity(entity_id: str) -> Dict:
    gathered_connections = {}
    entity_neighbours = get_entity_one_hop_neighbours(entity_id)
    for neighbour in entity_neighbours:
        connection_property = neighbour["connection_property"]
        connected_entity = neighbour["connected_entity"]
        if connection_property not in gathered_connections:
            gathered_connections[connection_property] = {}

        if connected_entity in gathered_connections[connection_property]:
            gathered_connections[connection_property][connected_entity] += 1
        else:
            gathered_connections[connection_property][connected_entity] = 1

    return gathered_connections


def gather_answers_connections(llm_predicted_answers_entities, n_jobs=4) -> Dict:
    gathered_connections = {}
    parallel = Parallel(n_jobs=n_jobs)

    connections_gatherer = parallel(
        delayed(__get_connections_of_entity)(entity_id) for entity_id in llm_predicted_answers_entities
    )
    for connections in connections_gatherer:
        for connection_property, connected_entities in connections.items():
            if connection_property not in gathered_connections:
                gathered_connections[connection_property] = {}
            for connected_entity, connections_amount in connected_entities.items():
                if connected_entity in gathered_connections[connection_property]:
                    gathered_connections[connection_property][connected_entity] += connections_amount
                else:
                    gathered_connections[connection_property][connected_entity] = connections_amount

    return gathered_connections


def build_entity_signature(gathered_connections) -> OrderedDict:
    signature_table = {}
    for connection_property, connected_items in gathered_connections.items():
        best_connected_entity = None
        best_connected_entity_count = 0
        for connected_entity, amount in connected_items.items():
            if best_connected_entity_count < amount:
                best_connected_entity = connected_entity
                best_connected_entity_count = amount

        # we ignore real value entities
        if not (best_connected_entity.startswith("Q") or best_connected_entity.startswith("q")):
            continue

        signature_table[connection_property] = (best_connected_entity, best_connected_entity_count)

    # Sort connections in signature
    tmp_for_sort = [item for item in signature_table.items()]
    tmp_for_sort.sort(key=lambda x: x[1][1], reverse=True)
    signature_table = OrderedDict(tmp_for_sort)

    return signature_table


def find_neighbour_by_signature(
        signature_table,
        question_entity,
        llm_predicted_answers_entities,
        llm_predicted_answers,
        top_n_signatures=0,
        take_all_signature_rules_with_full_match=True,
):
    # get all neighbours of entity with signature
    signature_conditions = [
        SparqlCondition(connection=item[0], destination=item[1][0], union_with_invert=True)
        for index, item in enumerate(signature_table.items())
        # take top n records from signature table or signatures matched by all answers
        if (index < top_n_signatures)
           or (take_all_signature_rules_with_full_match and item[1][1] == len(llm_predicted_answers))
    ]
    question_entity_neighbours = get_entity_one_hop_neighbours(
        question_entity,
        direct_only=True,
        conditions=signature_conditions
    )

    # sort neighbours by signature rating and choose the best one
    answer = None
    answer_entity = None
    for neighbour in question_entity_neighbours:
        if not (neighbour["connected_entity"].startswith("Q") or neighbour["connected_entity"].startswith("q")):
            # we don't need simple values, only entities
            continue

        # TODO: we just take the first one
        if neighbour["connected_entity"] in llm_predicted_answers_entities:
            answer_entity = neighbour["connected_entity"]
            answer_index = llm_predicted_answers_entities.index(answer_entity)
            answer = llm_predicted_answers[answer_index]
            break

    return answer, answer_entity
