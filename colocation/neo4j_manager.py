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

    def __init__(self, delete_nodes: bool = True):
        """
        constructor
        connects to neo4j graph and deletes all remaining nodes

        Args:
            delete_nodes(bool): flag whether to delete all nodes in the graph for a clean start.
        """
        # self.graph = Graph("bolt://localhost:7687", auth=('neo4j', password))
        self.graph = Graph()
        if delete_nodes:
            self.graph.delete_all()

    def add_matched_nodes_undirected(self, matched: pd.DataFrame, key_w: str, key_c: str,
                                     source_w: str, source_c: str,
                                     type_w: str = "workshop", type_c: str = "conference",
                                     relation_type: str = "matches"):
        """
        performs add_matched_nodes in both directions to get an undirected relation.

        Args:
            matched(df.DataFrame): match (result of a workshop conference match)
            key_w(str): column with the identifier for the 'workshops'
            key_c(str): column with the identifier for the 'conferences'
            source_w(str): datasource of the 'workshops' (e.g. wikidata, dblp, ...)
            source_c(str): datasource of the 'conferences'
            type_w(str): type of dataobject with W. prefix from 'workshop', 'conference'
            type_c(str): type of dataobject with C. prefix
        """
        self.add_matched_nodes(matched=matched, key_w=key_w, key_c=key_c,
                               source_w=source_w, source_c=source_c, type_w=type_w, type_c=type_c,
                               relation_type=relation_type)

        matched = matched.rename(
            columns={old: f"C.{old[2:]}" if old[0] == "W" else f"W.{old[2:]}" for old in matched.columns}
        )

        self.add_matched_nodes(matched=matched, key_w=key_c, key_c=key_w,
                               source_w=source_c, source_c=source_w, type_w=type_c, type_c=type_w,
                               relation_type=relation_type)

    def add_matched_nodes(self, matched: pd.DataFrame, key_w: str, key_c: str,
                          source_w: str, source_c: str,
                          type_w: str = "workshop", type_c: str = "conference",
                          relation_type: str = "matches"):
        """
        takes dataframe (from a crossover) matching by Matcher and adds the match to the database
        makes sure that nodes and relationships stay unique

        Args:
            matched(df.DataFrame): match (result of a workshop conference match)
            key_w(str): column with the identifier for the 'workshops'
            key_c(str): column with the identifier for the 'conferences'
            source_w(str): datasource of the 'workshops' (e.g. wikidata, dblp, ...)
            source_c(str): datasource of the 'conferences'
            type_w(str): type of dataobject with W. prefix from 'workshop', 'conference'
            type_c(str): type of dataobject with C. prefix
        """

        graph = self.graph

        conference_attr = [s for s in matched.columns if s[0] == "C" and s != f"C.{key_c}"]

        workshop_nodes = {}
        conference_nodes = {}

        tx = graph.begin()
        for row in matched.drop_duplicates(subset=[f"W.{key_w}"]).to_dict(orient="records"):
            node = Node(source_w, type_w,  # title=f"{key_w} {row[f'W.{key_w}']}",
                        **{f"{source_w}": row[f"W.{key_w}"]})
            workshop_nodes[row[f"W.{key_w}"]] = node
            tx.merge(node, type_w, source_w)

        for row in matched.drop_duplicates(subset=[f"C.{key_c}"]).to_dict(orient="records"):
            attributes = {key[2:]: value for key, value in row.items() if key in conference_attr}
            attributes[source_c] = row[f"C.{key_c}"]
            node = Node(source_c, key_c, **attributes)
            conference_nodes[row[f"C.{key_c}"]] = node
            tx.merge(node, type_c, source_c)

        graph.commit(tx)

        relation_type = relation_type.upper()
        MATCHES = Relationship.type(relation_type)
        tx = graph.begin()

        for row in matched.to_dict(orient="records"):
            work_node = workshop_nodes[row[f"W.{key_w}"]]
            conf_node = conference_nodes[row[f"C.{key_c}"]]
            m = MATCHES(work_node, conf_node)
            tx.merge(m)

        graph.commit(tx)
