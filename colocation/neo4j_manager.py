'''
Created on 2023-07-21
@author: nm
'''
from py2neo import Graph, Node, Relationship
from py2neo.bulk import merge_nodes
from .cache_manager import CsvCacheManager, JsonCacheManager
import pandas as pd
from typing import List, Tuple


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
        self.result_serializer = JsonCacheManager(base_folder="results")
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
            attributes = {key[2:]: value for key, value in row.items()
                          if key in conference_attr and not pd.isnull(value)}
            attributes[source_c] = row[f"C.{key_c}"]
            node = Node(source_c, type_c, **attributes)
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

    @staticmethod
    def wrap_name(*names: Tuple[str]) -> Tuple[str]:
        """
        Helper function that wraps the given names in ``
        to use in Cypher queries
        """
        return tuple(f"`{name}`" for name in names)

    def delete_match_when_linked(self, type_a: str, type_b: str):
        """
        Matches instances where a type_a node matches a type_b one,
        which is however already linked to a type_a node.
        Then deletes the corresponding matches relationship, since the
        stronger linked relationship exists

        Args:
            type_a(str): type of source eg 'Wikidata'
            type_b(str): type of target typically linked eg 'Dblp'
        """
        type_a, type_b = self.wrap_name(type_a, type_b)

        delete_query = f"""
match (a:{type_a})-[p:MATCHES]-(d:{type_b})
where (d)-[:LINKED]->(:{type_a})
delete p
"""
        self.graph.run(delete_query)

    def transfer_link(self, type_a: str, type_b: str):
        """
        Matches instances where a type_a node is linked to a type_b one,
        which is also linked to another node c of the same type.
        In this case, create a link between a and c.

        Args:
            type_a(str): type of source eg 'Wikidata' or 'workshop'
            type_b(str): type of transitive targets eg 'Dblp'
        """
        type_a, type_b = self.wrap_name(type_a, type_b)

        transfer_query = f"""
match (a:{type_a})-[:LINKED]->(b:{type_b})-[:LINKED]->(c:{type_b})
create (a)-[:LINKED]->(c)
create (a)<-[:LINKED]-(c)
"""
        self.graph.run(transfer_query)

    def check_uniqueness_workshop_connectivity(
            self, type_workshop: str, type_matched: str, type_linked: str, threshold: int = 3) -> List[str]:
        """
        When using the heuristic, that a certain number of workshops that are matched to one node
        and linked to the other node sufices to establish a link between the two nodes, the node of type a
        may be linked to two entirely different nodes due to incorrect data.
        This check looks for this pattern and returns which nodes need to be ignored in the heuristic.
        It also informs the user via the command line and saves the conflict.
        Does not automatically wrap types in ``, so when using manually, use f.e. `Ceur-Ws` when name contains dash.

        Args:
            type_workshop(str): type of the workshop like entity with links and matches eg 'Ceur-Ws'.
            type_matched(str): type of the nodes matched by workshops eg 'Wikidata'.
            type_linked(str): type of the nodes linked by workshops eg 'Dblp'.
            threshold(int): minimum number of workshops to consider connecting with the heuristic.
        Returns:
            list(str): list of the node of type 'type_linked' which should be excluded from the heuristic.
        """

        check_query = f"""
match (w1:{type_matched})<-[:MATCHES]-(a:{type_workshop})-[:LINKED]->(d:{type_linked})
with d, w1, count(a) as c1
match (w2:{type_matched})<-[:MATCHES]-(b:{type_workshop})-[:LINKED]->(d:{type_linked})
with d, w1, w2, c1, count(b) as c2
where c1 > {threshold - 1} and c2 > {threshold - 1} and w1 < w2 and not d:Virtual
return d, w1, w2
    """
        data = self.graph.run(check_query).data()
        res = []

        if not len(data) == 0:
            print("\nThe connectivity heuristic would link one node to at least two different ones.")
            print("The results problematic data will be saved in the home directory in .ceurws/conflicts.csv")
            print("Check if perhaps some incorrect data is present and correct it.")
            print("Conflict nodes will be ignored in the heuristic.")
            cacher = CsvCacheManager()
            cacher.store_csv("conflicts", pd.DataFrame(data=data))

            # if the type was wrapped, we need to unwrap it to access the variable
            if type_linked[0] == "`":
                type_linked = type_linked[1:-1]
            # result may hold multiple occurances of the same linked entity
            res = list(set([result["d"][type_linked] for result in data]))

        return res

    def create_link_by_workshop_connectivity(
            self, type_workshop: str, type_matched: str, type_linked: str, threshold: int = 3):
        """
        Performs the heuristic on the graph, where if at least 'threashold' workshops are linked to
        one conference and matched to another one (of a different type), then the conferences should be the same.
        Uses 'check_uniqueness_workshop_connectivity' to exclude nodes,
        which would be linked to multiple different conferences.

        Args:
            type_workshop(str): type of the workshop like entity with links and matches eg 'Ceur-Ws'.
            type_matched(str): type of the nodes matched by workshops eg 'Wikidata'.
            type_linked(str): type of the nodes linked by workshops eg 'Dblp'.
            threshold(int): minimum number of workshops to consider connecting with the heuristic.
        """
        type_workshop, type_matched, type_linked = self.wrap_name(type_workshop, type_matched, type_linked)

        exclude_list = self.check_uniqueness_workshop_connectivity(
            type_workshop, type_matched, type_linked, threshold
        )
        exclude_list = [f'"{r}"' for r in exclude_list]  # put into quotes
        exclude_string = ", ".join(exclude_list)

        link_query = f"""
match (w:{type_matched})<-[:MATCHES]-(a:{type_workshop})-[:LINKED]->(d:{type_linked})
with w, d, count(a) as c
where c > {threshold - 1} and not (w)-[:LINKED]->(d) and not toString(d.{type_linked}) in [{exclude_string}]
create (w)-[:LINKED]->(d)
create (w)<-[:LINKED]-(d)
"""

        self.graph.run(link_query)

    def set_dblp_virtual(self):
        """
        Matches the virtual Dblp nodes and gives them the attribute 'Virtual'.
        """
        query = "match (r:Dblp)-[:LINKED]->(v:Dblp) set v:Virtual"
        self.graph.run(query)

    def add_ceur_attributes(self, volumes: List[dict], colocation_lod: List[dict]):
        """
        Matches the already present Ceur-WS volumes and adds their information
        into the neo4j graph for better result readability.

        Args:
            volumes(list(dict)): Ceur-WS volumes lod.
            colocation_lod(list(dict)): Ceur-Ws extract
        """
        # make a deep copy to modify the volume properties
        lod = [vol.copy() for vol in volumes]

        # map volume number to wikidata uri(s)
        number_wikidata_map = {volume["number"]: volume["wikidata_event"] for volume in colocation_lod}
        number_wikidata_map = {num: wiki if wiki else "" for num, wiki in number_wikidata_map.items()}

        number_proceedings_map = {volume["number"]: volume["wikidata_proceedings"] for volume in colocation_lod}

        # get the volumes actually present in the neo4j server
        data = self.graph.run("match (a:`Ceur-WS`) return a.`Ceur-WS`").data()
        present = [vol["a.`Ceur-WS`"] for vol in data]

        # only retain present volumes
        lod = [volume for volume in lod if volume["number"] in present]

        for volume in lod:
            volume["Ceur-WS"] = volume.pop("number")
            volume["Wikidata"] = number_wikidata_map[volume["Ceur-WS"]]
            volume["Proceedings"] = number_proceedings_map[volume["Ceur-WS"]]

        merge_nodes(self.graph.auto(), lod, merge_key=("Ceur-WS", "Ceur-WS"))

    def serialize_results(self):
        """
        Queries the different types of results from the neo4j database
        and serializes them via Json into a the folder $home/.ceurws/results.

        There are two orthogonal properties to consider.
        The first one is the quality of the result. Here we have how a workshop is connected:
        wikidata + dblp with edge between them > wikidata + dblp without edge between them >
        only dblp link > only wikidata match.
        The second one is, whether the wikidata event entry of the volume is present.
        """
        result_serializer = self.result_serializer
        graph = self.graph

        for wikidata_present, logic_symbol in zip(["event_present", "event_missing"], ["<>", "="]):

            types = ["fully_connected", "doubly_connected", "dblp_only", "wikidata_only"]
            queries = [
                f"""
match (wikidata:Wikidata)<-[:MATCHES]-(ceur:`Ceur-WS`)-[:LINKED]->(d:Dblp)
where (wikidata)-->(d) and ceur.Wikidata {logic_symbol} ''
return ceur, wikidata
                """,
                f"""
match (wikidata:Wikidata)<-[:MATCHES]-(ceur:`Ceur-WS`)-[:LINKED]->(d:Dblp)
where not (wikidata)-->(d) and ceur.Wikidata {logic_symbol} ''
return ceur, wikidata
                """,
                f"""
match (ceur:`Ceur-WS`)-[:LINKED]->(dblp:Dblp)
where not (:Wikidata)<-[:MATCHES]-(ceur) and ceur.Wikidata {logic_symbol} ''
return ceur, dblp
                """,
                f"""
match (wikidata:Wikidata)<-[:MATCHES]-(ceur:`Ceur-WS`)
where not (ceur)-[:LINKED]->(:Dblp) and ceur.Wikidata {logic_symbol} ''
return *
                """
            ]

            for typ, query in zip(types, queries):

                data = graph.run(query).to_data_frame().to_dict(orient='records')
                result_serializer.store_lod(f"{typ}_{wikidata_present}", data, indent=True)
