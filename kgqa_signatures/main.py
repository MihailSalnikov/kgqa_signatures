import dataclasses

from sklearn.metrics import precision_score

from kgqa_signatures.dataset import (
    DatasetRecord,
    apple_ml_mkqa,
    debug_data,
    mintaka,
    wikidata_simplequestions
)
from kgqa_signatures.signature import (
    build_entity_signature,
    find_neighbour_by_signature,
    gather_answers_connections
)
from kgqa_signatures.wikidata.service import get_entity_one_hop_neighbours
from kgqa_signatures.wikidata.sparql_condition import SparqlCondition


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
    gt_answers_entities = []
    estimated_answers_entities = []

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
        gathered_connections = gather_answers_connections(record.llm_predicted_answers_entities)

        # Step 3: build signature of good entity
        signature_table = build_entity_signature(gathered_connections)

        # Step 4: get best neighbour by signature
        answer, answer_entity = find_neighbour_by_signature(
            signature_table,
            record.question_entity,
            record.llm_predicted_answers_entities,
            record.llm_predicted_answers,
            top_n_signatures=0,
            take_all_signature_rules_with_full_match=True
        )

        # Step 5: check answer
        gt_answers_entities.append(record.answer_entity)
        estimated_answers_entities.append(answer_entity)
        is_correct = answer_entity == record.answer_entity
        print(f"Question: {record.question} Answer: {answer} Answer_entity: {answer_entity}"
              f" Is correct: {is_correct} Correct answer_entity: {record.answer_entity}")

    return precision_score(gt_answers_entities, estimated_answers_entities, average='micro')


if __name__ == "__main__":
    # dataset_records_provider = wikidata_simplequestions(
    #     filepath="data/wikidata_simplequestion/annotated_wd_data_valid_answerable.txt",
    #     llm_answers_filepath="data/wikidata_simplequestion/llm_result/sqwd_results_validation_with_el.jsonl"
    # )
    # dataset_records_provider = apple_ml_mkqa("data/apple-ml-mkqa/mkqa.jsonl")
    # dataset_records_provider = mintaka("data/mintaka/mintaka_test.json")
    dataset_records_provider = debug_data()
    precision = process_dataset(dataset_records_provider)
    print(f"Precision: {precision}")
