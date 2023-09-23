import time

import requests

from wikidata_processor.config import MEDIAWIKI_API_URL, SPARQL_API_URL
from wikidata_processor.logger import get_logger

logger = get_logger()


def search_entity(
        query: str,
        max_results: int = 500,
        language: str = "en",
        api_url: str = MEDIAWIKI_API_URL
):
    headers = {

    }
    params = {

    }
    reply = requests.get(api_url, params=params, headers=headers)

    print(reply)


def execute_wiki_request_with_delays(api_url, params, headers):
    response = requests.get(
        api_url,
        params=params,
        headers=headers,
    )
    to_sleep = 0.2
    while response.status_code == 429:
        logger.warning(
            {
                "msg": f"Request to wikidata endpoint failed. Retry.",
                "params": params,
                "endpoint": api_url,
                "response": {
                    "status_code": response.status_code,
                    "headers": dict(response.headers),
                },
                "retry_after": to_sleep,
            }
        )
        if "retry-after" in response.headers:
            to_sleep += int(response.headers["retry-after"])
        to_sleep += 0.5
        time.sleep(to_sleep)
        response = requests.get(
            api_url,
            params=params,
            headers=headers,
        )

    return response


def execute_sparql_request(request, api_url: str = SPARQL_API_URL):
    params = {"format": "json", "query": request}
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36",
    }
    logger.info(
        {
            "msg": "Send request to Wikidata",
            "params": params,
            "endpoint": api_url,
        }
    )
    response = execute_wiki_request_with_delays(api_url, params, headers)

    try:
        return response.json()["results"]["bindings"]
    except Exception as e:
        logger.error(
            {
                "msg": str(e),
                "params": params,
                "endpoint": api_url,
                "response": {
                    "status_code": response.status_code,
                    "headers": dict(response.headers),
                },
            }
        )
        raise e
