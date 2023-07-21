'''
Created on 2023-07-21
@author: nm
'''
from py2neo import Graph, Node, Relationship
import pandas as pd


class Neo4jManager:
    """
    Manages importing data and queries to the neo4j graph
    accumulating the results of the different matching steps.
    """

    def __init__(self, password: str = None):
        """
        constructor
        connects to neo4j graph and deletes all remaining nodes

        Args:
            password(str): password for the neo4j server if required
        """
        self.graph = Graph("bolt://localhost:7687", auth=('neo4j', password))
        self.graph.delete_all()

    def add_matched_nodes(self, matched: pd.DataFrame, key_w: str, key_c: str, source_w: str, source_c: str):
        """
        takes dataframe from a crossover matching by Matcher and adds the match to the database
        makes sure that nodes and relationships stay unique

        Args:
            matched(df.DataFrame): match result of a workshop conference match
            key_w(str): column with the identifier for the workshops
            key_c(str): column with the identifier for the conferences
            source_w(str): datasource of the workshops (e.g. wikidata, dblp, ...)
            source_c(str): datasource of the conferences
        """

        graph = self.graph

        conference_attr = [s for s in matched.columns if s[0] == "C"]

        workshop_nodes = {}
        conference_nodes = {}

        tx = graph.begin()
        for row in matched.drop_duplicates(subset=[f"W.{key_w}"]).to_dict(orient="records"):
            node = Node(source_w, 'workshop', title=f"{key_w} {row[f'W.{key_w}']}",
                        **{f"id_{source_w}": row[f"W.{key_w}"]})
            workshop_nodes[row[f"W.{key_w}"]] = node
            tx.merge(node, "workshop", f"id_{source_w}")

        for row in matched.drop_duplicates(subset=[f"C.{key_c}"]).to_dict(orient="records"):
            attributes = {key[2:]: value for key, value in row.items() if key in conference_attr}
            attributes[f"id_{source_c}"] = row[f"C.{key_c}"]
            node = Node(source_c, key_c, **attributes)
            conference_nodes[row[f"C.{key_c}"]] = node
            tx.merge(node, "conference", key_c)

        graph.commit(tx)

        MATCHES = Relationship.type("MATCHES")
        tx = graph.begin()

        for row in matched.to_dict(orient="records"):
            work_node = workshop_nodes[row[f"W.{key_w}"]]
            conf_node = conference_nodes[row[f"C.{key_c}"]]
            m = MATCHES(work_node, conf_node)
            tx.merge(m)

        graph.commit(tx)
