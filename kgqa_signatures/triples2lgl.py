if __name__ == "__main__":
    nodes_start = {}
    with open("wikidata_triples.txt", "r") as f:
        with open("wikidata_lgl.txt", "w") as w:
            current_node = None
            node_to_edges = {}
            node_edges = set()
            for line in f:
                start_node, property, end_node = line.replace("\n", "").split(" ")
                if current_node is None:
                    current_node = start_node
                    if current_node in nodes_start:
                        nodes_start[current_node] += 1
                    else:
                        nodes_start[current_node] = 1

                if start_node != current_node:
                    if current_node not in node_to_edges:
                        node_to_edges[current_node] = []
                    node_to_edges[current_node].append(node_edges)
                    node_edges = set()
                    current_node = start_node
                    if current_node in nodes_start:
                        nodes_start[current_node] += 1
                    else:
                        nodes_start[current_node] = 1

                node_edges.add(f"{end_node} {property}")

            for root_node, edges_groups in node_to_edges.items():
                w.write("\n")
                w.write(f"# {root_node}")
                final_group = set()
                for edge_group in edges_groups:
                    for str in edge_group:
                        final_group.add(str)
                for str in final_group:
                    w.write(f"\n{str}")

    for k, v in nodes_start.items():
        if v > 1:
            print(k, v)
