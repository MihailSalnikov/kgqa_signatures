import dataclasses
from collections import OrderedDict
from typing import Dict

from joblib import Parallel, delayed

from wikidata_processor.dataset import wikidata_simplequestions, debug_data, apple_ml_mkqa, mintaka, DatasetRecord
from wikidata_processor.wikidata.service import get_entity_one_hop_neighbours
from wikidata_processor.wikidata.sparql_condition import SparqlCondition


def llm(question: str):
    return [
        "Washington",
        "New York",
        "Los-Angeles",
        "Chicago",
        "Houston",
    ]


def entity_linker_answers(sentences_with_entity: [str]):
    return [
        "Q61",
        "Q60",
        "Q65",
        "Q1297",
        "Q16555",
    ]


def entity_linker_question(sentence_with_entity: str):
    return "Q30"


def process_dataset(dataset_records_provider):
    for record in dataset_records_provider:
        # Step 0: some datasets don't provide entity for question, so we detect it ourselves
        if record.question_entity is None:
            question_entity = entity_linker_question(record.question)
            record = dataclasses.replace(record, question_entity=question_entity)

        # Step 1: infer LLM to produce answer variants
        if record.llm_predicted_answers is None or record.llm_predicted_answers_entities is None:
            answers = llm(record.question)
            answers_entities = entity_linker_answers(answers)
            record = dataclasses.replace(
                record,
                llm_predicted_answers=answers,
                llm_predicted_answers_entities=answers_entities
            )

        # Step 2: gather neighbours and connections of entities from all answers to common table
        # TODO: SPARSQL returns the last object connected with from list (for example city with many head of governments)
        gathered_connections = {}
        parallel = Parallel(n_jobs=4)

        def get_connections_of_entity(entity_id: str) -> Dict:
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

        connections_gatherer = parallel(
            delayed(get_connections_of_entity)(entity_id) for entity_id in record.llm_predicted_answers_entities
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

        # Step 3: build signature of good entity
        signature_table = {}
        for connection_property, connected_items in gathered_connections.items():
            best_connected_entity = None
            best_connected_entity_count = 0
            for connected_entity, amount in connected_items.items():
                if best_connected_entity_count < amount:
                    best_connected_entity = connected_entity
                    best_connected_entity_count = amount

            # we ignore real value entities
            if not(best_connected_entity.startswith("Q") or best_connected_entity.startswith("q")):
                continue

            signature_table[connection_property] = (best_connected_entity, best_connected_entity_count)

        # Sort connections in signature
        tmp_for_sort = [item for item in signature_table.items()]
        tmp_for_sort.sort(key=lambda x: x[1][1], reverse=True)
        signature_table = OrderedDict(tmp_for_sort)

        # Step 4: search entity in question by entity linker
        entity_from_question = record.question_entity

        # Step 5: get all neighbours of entity with signature
        TOP_N_SIGNATURES = 0
        signature_conditions = [
            SparqlCondition(connection=item[0], destination=item[1][0], union_with_invert=True)
            for index, item in enumerate(signature_table.items())
            # take top n records from signature table or signatures matched by all answers
            if (index < TOP_N_SIGNATURES) or (item[1][1] == len(record.llm_predicted_answers))
        ]
        question_entity_neighbours = get_entity_one_hop_neighbours(
            entity_from_question,
            direct_only=True,
            conditions=signature_conditions
        )

        # Step 6: sort neighbours by signature rating and choose the best one
        answer = None
        answer_entity = None
        for neighbour in question_entity_neighbours:
            if not (neighbour["connected_entity"].startswith("Q") or neighbour["connected_entity"].startswith("q")):
                # we don't need simple values, only entities
                continue

            # TODO: we just take the first one
            if neighbour["connected_entity"] in record.llm_predicted_answers_entities:
                answer_entity = neighbour["connected_entity"]
                answer_index = record.llm_predicted_answers_entities.index(answer_entity)
                answer = record.llm_predicted_answers[answer_index]
                break

        # Step 7: check answer
        is_correct = answer == record.answer_entity
        print(f"Question: {record.question} Answer: {answer} Answer_entity: {answer_entity}"
              f" Is correct: {is_correct} Correct answer_entity: {record.answer_entity}")


if __name__ == "__main__":
    # dataset_records_provider = wikidata_simplequestions(
    #     filepath="data/wikidata_simplequestion/annotated_wd_data_valid_answerable.txt",
    #     llm_answers_filepath="data/wikidata_simplequestion/llm_result/sqwd_results_validation_with_el.jsonl"
    # )
    # dataset_records_provider = apple_ml_mkqa("data/apple-ml-mkqa/mkqa.jsonl")
    # dataset_records_provider = mintaka("data/mintaka/mintaka_test.json")
    dataset_records_provider = debug_data()
    process_dataset(dataset_records_provider)
