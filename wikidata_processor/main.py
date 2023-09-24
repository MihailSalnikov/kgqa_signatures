import dataclasses

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
        answers = llm(record.question)

        # Step 2: search entity in text by entity linker
        answers_entities = entity_linker_answers(answers)

        # Step 3: gather neighbours and connections of entities from all answers to common table
        # TODO: SPARSQL returns the last object connected with from list (for example city with many head of governments)
        gathered_connections = {}
        for entity_id in answers_entities:
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

        # Step 4: build signature of good entity
        signature_table = {}
        for connection_property, connected_items in gathered_connections.items():
            best_connected_entity = None
            best_connected_entity_count = 0
            for connected_entity, amount in connected_items.items():
                if best_connected_entity_count < amount:
                    best_connected_entity = connected_entity
                    best_connected_entity_count = amount

            if best_connected_entity_count < len(answers_entities):
                continue

            signature_table[connection_property] = (best_connected_entity, best_connected_entity_count)

        # Step 5: search entity in question by entity linker
        entity_from_question = record.question_entity

        # Step 6: get all neighbours of entity with signature
        signature_conditions = [
            SparqlCondition(connection=connection, destination=entity[0], union_with_invert=True)
            for connection, entity in signature_table.items()
            if entity[0].startswith("Q") or entity[0].startswith("q")  # TODO: we ignore real value entities
        ]
        question_entity_neighbours = get_entity_one_hop_neighbours(
            entity_from_question,
            direct_only=True,
            conditions=signature_conditions
        )

        # Step 7: sort neighbours by signature rating and choose the best one
        answer = None
        for neighbour in question_entity_neighbours:
            if not (neighbour["connected_entity"].startswith("Q") or neighbour["connected_entity"].startswith("q")):
                # we don't need simple values, only entities
                continue

            # TODO: we just take the first one
            if neighbour["connected_entity"] in answers_entities:
                answer = neighbour["connected_entity"]
                break

        # Step 8: check answer
        is_correct = answer == record.answer_entity
        print(f"Question: {record.question} Answer: {answer} Is correct: {is_correct} Correct answer: {record.answer_entity}")


if __name__ == "__main__":
    # dataset_records_provider = wikidata_simplequestions("data/wikidata_simplequestion/annotated_wd_data_test_answerable.txt")
    # dataset_records_provider = apple_ml_mkqa("data/apple-ml-mkqa/mkqa.jsonl")
    # dataset_records_provider = mintaka("data/mintaka/mintaka_test.json")
    dataset_records_provider = debug_data()
    process_dataset(dataset_records_provider)
