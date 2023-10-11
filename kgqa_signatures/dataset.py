import csv
import json
from dataclasses import dataclass
from typing import Union, List


@dataclass
class DatasetRecord:
    question: str
    question_entity: Union[str, None]
    answer_entity: Union[str, None]
    llm_predicted_answers: Union[List[str], None] = None
    llm_predicted_answers_entities: Union[List[str], None] = None
    question_to_answer_connection: str = None
    answer_to_question_connection: str = None


def debug_data():
    for _ in range(2):
        yield DatasetRecord(
            question="What is the capital of the USA?",
            question_entity="Q30",
            answer_entity="Q61",
            llm_predicted_answers=[
                "Washington",
                "New York",
                "Los-Angeles",
                "Chicago",
                "Houston",
            ],
            llm_predicted_answers_entities=[
                "Q61",
                "Q60",
                "Q65",
                "Q1297",
                "Q16555",
            ],
            question_to_answer_connection="P36"
        )


def __get_llm_results_provider(llm_answers_filepath: str = None):
    if llm_answers_filepath is not None:
        with open(llm_answers_filepath) as llm_file:
            for line in llm_file:
                record = json.loads(line)
                yield {
                    'answers': record['answer_llm'],
                    'answer_ids': record['answer_ids']
                }

    # if no file or it ends just spam with None
    while True:
        yield {
            'answers': None,
            'answer_ids': None
        }


def wikidata_simplequestions(filepath: str, llm_answers_filepath: str = None):
    llm_results_provider = __get_llm_results_provider(llm_answers_filepath)
    with open(filepath) as csvfile:
        reader = csv.reader(csvfile, delimiter="\t")
        for row in reader:
            llm_result = llm_results_provider.__next__()
            question_entity, connection, answer_entity, question = row
            yield DatasetRecord(
                question,
                question_entity,
                answer_entity,
                llm_predicted_answers=llm_result['answers'],
                llm_predicted_answers_entities=llm_result['answer_ids'],
                question_to_answer_connection=connection if connection[0] == "P" else None,
                answer_to_question_connection=f"P{connection[1:]}" if connection[0] == "R" else None,
            )


def apple_ml_mkqa(filepath: str, llm_answers_filepath: str = None):
    llm_results_provider = __get_llm_results_provider(llm_answers_filepath)
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            llm_result = llm_results_provider.__next__()
            record_json = json.loads(line)
            if record_json["answers"]["en"][0]["type"] != "entity":
                continue
            yield DatasetRecord(
                question=record_json["query"],
                question_entity=None,  # TODO: FIX!!!! Dataset don't provide entity in question
                answer_entity=record_json["answers"]["en"][0]["entity"],
                llm_predicted_answers=llm_result['answers'],
                llm_predicted_answers_entities=llm_result['answer_ids'],
            )


def mintaka(filepath: str,  llm_answers_filepath: str = None):
    llm_results_provider = __get_llm_results_provider(llm_answers_filepath)
    with open(filepath, "r", encoding="utf-8") as f:
        records_json = json.load(f)
        for record_json in records_json:
            llm_result = llm_results_provider.__next__()
            if record_json["answer"]["answerType"] != "entity":
                continue
            yield DatasetRecord(
                question=record_json["question"],
                question_entity=record_json["questionEntity"][0]["name"],  # TODO: Which to choose?? Dataset provides multiple entities in question
                answer_entity=record_json["answer"]["answer"][0]["name"],
                llm_predicted_answers=llm_result['answers'],
                llm_predicted_answers_entities=llm_result['answer_ids'],
            )


