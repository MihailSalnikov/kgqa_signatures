class SparqlCondition:
    def __init__(self, source=None, connection=None, destination=None, union_with_invert=False):
        self.source = source
        self.connection = connection
        self.destination = destination
        self.union_with_invert = union_with_invert

    def to_sparql_query_condition(self, source_var=None, connection_var=None, destination_var=None):
        if self.source is None:
            source = source_var
        else:
            source = self.source
        if self.connection is None:
            connection = connection_var
        elif self.connection.startswith("P") or self.connection.startswith("p"):
            connection = "wdt:" + self.connection
        else:
            connection = self.connection
        if self.destination is None:
            destination = destination_var
        elif self.destination.startswith("Q") or self.destination.startswith("q"):
            destination = "wd:" + self.destination
        else:
            destination = self.destination

        condition = f"{source} {connection} {destination}"
        inverted_condition = f"{destination} {connection} {source}"
        if (self.destination.startswith("Q") or self.destination.startswith("q")) and self.union_with_invert:
            return "{" + condition + "} UNION {" + inverted_condition + "}."
        else:
            return condition + "."
