import csv
import json
from dataclasses import dataclass


@dataclass
class DatasetRecord:
    question: str
    question_entity: str
    answer_entity: str
    question_to_answer_connection: str = None
    answer_to_question_connection: str = None


def debug_data():
    yield DatasetRecord(
        question="What is the capital of the USA?",
        question_entity="Q30",
        answer_entity="Q61",
        question_to_answer_connection="P36"
    )


def wikidata_simplequestions(filepath: str):
    with open(filepath) as csvfile:
        reader = csv.reader(csvfile, delimiter="\t")
        for row in reader:
            question_entity, connection, answer_entity, question = row
            yield DatasetRecord(
                question,
                question_entity,
                answer_entity,
                question_to_answer_connection=connection if connection[0] == "P" else None,
                answer_to_question_connection=f"P{connection[1:]}" if connection[0] == "R" else None,
            )


def apple_ml_mkqa(filepath: str):
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            record_json = json.loads(line)
            if record_json["answers"]["en"][0]["type"] != "entity":
                continue
            yield DatasetRecord(
                question=record_json["query"],
                question_entity=None,  # TODO: FIX!!!! Dataset don't provide entity in question
                answer_entity=record_json["answers"]["en"][0]["entity"]
            )


def mintaka(filepath: str):
    with open(filepath, "r", encoding="utf-8") as f:
        records_json = json.load(f)
        for record_json in records_json:
            if record_json["answer"]["answerType"] != "entity":
                continue
            yield DatasetRecord(
                question=record_json["question"],
                question_entity=record_json["questionEntity"][0]["name"],  # TODO: Which to choose?? Dataset provides multiple entities in question
                answer_entity=record_json["answer"]["answer"][0]["name"]
            )


