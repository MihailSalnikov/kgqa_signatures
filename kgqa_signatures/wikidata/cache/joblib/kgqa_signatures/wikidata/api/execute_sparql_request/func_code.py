# first line: 46
@memory.cache(ignore=['api_url'])
def execute_sparql_request(request: str, api_url: str = SPARQL_API_URL):
    params = {"format": "json", "query": request}
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36",
    }
    logger.info(
        {
            "msg": "Send request to Wikidata",
            "params": params,
            "endpoint": api_url,
            "request": request
        }
    )
    response = execute_wiki_request_with_delays(api_url, params, headers)

    try:
        response = response.json()["results"]["bindings"]
        logger.debug(
            {
                "msg": "Received response from Wikidata",
                "params": params,
                "endpoint": api_url,
                "request": request,
                "response": response
            }
        )
        return response
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
