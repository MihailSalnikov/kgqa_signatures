from typing import List, Union, Dict, Iterable

from kgqa_signatures.logger import get_logger
from kgqa_signatures.wikidata.api import execute_sparql_request
from kgqa_signatures.wikidata.sparql_condition import SparqlCondition

logger = get_logger()


def get_entity_one_hop_neighbours(
        entity_id: str,
        direct_only: bool = False,
        conditions: Union[List[SparqlCondition], None] = None,
        match_all_conditions: bool = True,
):
    if conditions is None:
        conditions = []
    conditions = map(
        lambda x: x.to_sparql_query_condition(
            source_var="?object",
            connection_var="?property",
            with_end_symbol=match_all_conditions
        ),
        conditions
    )
    join_symbol = "\n" if match_all_conditions else " UNION "
    rendered_conditions = join_symbol.join(conditions)

    # when entity or property is the root of relation
    # it is depicted here as Qxxx-xxxx-xxx or Pxxx-xxx-xxx instead of Qxxx or Pxxx
    # also we skip all not entity objects by rule '?object wdt:P31 ?smth.'
    sparql_query_all = """
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wd: <http://www.wikidata.org/entity/>
SELECT DISTINCT ?property ?object
WHERE {
    {?object ?property wd:<ENTITY>} UNION {wd:<ENTITY> ?property ?object}.
    ?object wdt:P31 ?smth.
    <CONDITIONS>
}
    """.replace("<ENTITY>", entity_id).replace("<CONDITIONS>", rendered_conditions)
    sparql_query_direct = """
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wd: <http://www.wikidata.org/entity/>
SELECT DISTINCT ?property ?object
WHERE {
    wd:<ENTITY> ?property ?object.
    ?object wdt:P31 ?smth.
    <CONDITIONS>
}
    """.replace("<ENTITY>", entity_id).replace("<CONDITIONS>", rendered_conditions)
    sparql_query = sparql_query_direct if direct_only else sparql_query_all
    result = execute_sparql_request(sparql_query)

    if result is None:
        logger.error(
            {
                "msg": "cached request was with error",
                "query": sparql_query,
            }
        )
        return {}

    parsed_result = []
    for item in result:
        connection_property = item["property"]["value"]
        connection_property = connection_property[connection_property.rfind('/') + 1:]

        connected_entity = item["object"]["value"]
        connected_entity = connected_entity[connected_entity.rfind('/') + 1:]

        parsed_result.append((connection_property, connected_entity))

    return parsed_result


def count_matches(candidates: Iterable[str], conditions: List[SparqlCondition]) -> Dict[str, int]:
    conditions = map(
        lambda x: x.to_sparql_query_condition(
            source_var="?object",
            with_end_symbol=False
        ),
        conditions
    )
    rendered_conditions = " UNION ".join(conditions)

    candidates = map(
        lambda x: f"wd:{x}",
        candidates
    )
    rendered_candidates = " ".join(candidates)
    sparql_query = """
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wd: <http://www.wikidata.org/entity/>
SELECT ?object (COUNT(?object) as ?matched)
WHERE {
    <CONDITIONS>
    VALUES ?object { <CANDIDATES> }
}
GROUP BY ?object""".replace("<CONDITIONS>", rendered_conditions).replace("<CANDIDATES>", rendered_candidates)

    result = execute_sparql_request(sparql_query)

    if result is None:
        logger.error(
            {
                "msg": "cached request was with error",
                "query": sparql_query,
            }
        )
        return {}

    parsed_result = {}
    for item in result:
        matches_count = int(item['matched']['value'])
        entity = item['object']['value'].split("/")[-1]
        if entity.startswith("Q") or entity.startswith("q"):
            parsed_result[entity] = matches_count

    return parsed_result

