from typing import List, Union

from wikidata_processor.wikidata.api import execute_sparql_request
from wikidata_processor.logger import get_logger
from wikidata_processor.wikidata.sparql_condition import SparqlCondition

logger = get_logger()


def get_entity_one_hop_neighbours(
        entity_id: str,
        direct_only: bool = False,
        conditions: Union[List[SparqlCondition], None] = None
):
    if conditions is None:
        conditions = []
    conditions = map(
        lambda x: x.to_sparql_query_condition(
            source_var="?object",
            connection_var="?property"
        ),
        conditions
    )
    rendered_conditions = "\n".join(conditions)

    sparql_query_all = """
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wd: <http://www.wikidata.org/entity/>
SELECT DISTINCT ?property ?object
WHERE {
    {?object ?property wd:<ENTITY>} UNION {wd:<ENTITY> ?property ?object}.
    <CONDITIONS>
}
    """.replace("<ENTITY>", entity_id).replace("<CONDITIONS>", rendered_conditions)
    sparql_query_direct = """
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wd: <http://www.wikidata.org/entity/>
SELECT DISTINCT ?property ?object
WHERE {
    wd:<ENTITY> ?property ?object.
    <CONDITIONS>
}
    """.replace("<ENTITY>", entity_id).replace("<CONDITIONS>", rendered_conditions)
    sparql_query = sparql_query_direct if direct_only else sparql_query_all
    result = execute_sparql_request(sparql_query)

    parsed_result = []
    for item in result:
        connection_property = item["property"]["value"]
        connection_property = connection_property.split("/")[-1]

        if not connection_property.startswith("P"):
            # TODO: filter in SPARQL query somehow --- some extra info is here
            continue

        connected_entity = item["object"]["value"]
        connected_entity = connected_entity.split("/")[-1]
        # when entity or property is the root of relation
        # it is depicted here as Qxxx-xxxx-xxx or Pxxx-xxx-xxx instead of Qxxx or Pxxx
        if (connected_entity.startswith("Q") or connected_entity.startswith("q") or connected_entity.startswith(
                "P") or connected_entity.startswith("p")) and "-" in connected_entity:
            # TODO: it is connection from qualifier -- skip for now, but maybe need later
            continue

        parsed_result.append({
            "connected_entity": connected_entity,
            "connection_property": connection_property
        })

    return parsed_result
