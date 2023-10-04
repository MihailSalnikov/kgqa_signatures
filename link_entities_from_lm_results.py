from tqdm.auto import tqdm
import pandas as pd
from argparse import ArgumentParser
from joblib import Memory

import requests
import os
import itertools

tqdm.pandas()

SPARQL_ENDPOINT = os.environ.get("SPARQL_ENDPOINT", "https://query.wikidata.org/sparql")
WIKIDATA_URI = os.environ.get("WIKIDATA_URI", "https://www.wikidata.org/")
DEFAULT_CACHE_PATH = "./web_cache"

LOG_FILENAME = "log.json"

parse = ArgumentParser()
parse.add_argument("seq2seq_results_path", help="Path to results.csv from seq2seq")
parse.add_argument(
    "output_path", help="Path to results with linker entities in JSONL format"
)

memory = Memory(DEFAULT_CACHE_PATH, verbose=0)


@memory.cache
def get_wd_search_results(
    search_string: str,
    max_results: int = 10,
    language: str = "en",
    mediawiki_api_url: str = "https://www.wikidata.org/w/api.php",
    user_agent: str = None,
) -> list:
    params = {
        "action": "wbsearchentities",
        "language": language,
        "search": search_string,
        "format": "json",
        "limit": 5,
    }

    user_agent = "pywikidata" if user_agent is None else user_agent
    headers = {"User-Agent": user_agent}

    cont_count = 1
    results = []
    while cont_count > 0:
        params.update({"continue": 0 if cont_count == 1 else cont_count})

        reply = requests.get(mediawiki_api_url, params=params, headers=headers)
        reply.raise_for_status()
        search_results = reply.json()

        if search_results["success"] != 1:
            raise Exception("WD search failed")
        else:
            for i in search_results["search"]:
                results.append(i["id"])

        if "search-continue" not in search_results:
            cont_count = 0
        else:
            cont_count = search_results["search-continue"]

        if cont_count > max_results:
            break

    return results


def _text_answers_to_entity_answers(row):
    return list(
        itertools.chain(*[get_wd_search_results(answer)[:1] for answer in row.values])
    )


def main(args):
    df = pd.read_csv(args.seq2seq_results_path)
    answer_cols = df.columns[2:]

    df["answer_ids"] = df[answer_cols].progress_apply(
        _text_answers_to_entity_answers, axis=1
    )
    df["answer_llm"] = df[answer_cols].apply(lambda row: row.values, axis=1)

    df[["question", "target", "answers_llm", "answer_ids"]].to_json(
        args.output_path, lines=True, orient="records"
    )


if __name__ == "__main__":
    args = parse.parse_args()
    main(args)
