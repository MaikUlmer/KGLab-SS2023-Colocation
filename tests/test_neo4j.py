'''
Created on 2023-07-21

@author: nm
'''
import unittest
import os
import pandas as pd
from io import StringIO
from py2neo import Graph
from colocation.neo4j_manager import Neo4jManager
from colocation.matcher import Matcher

test_data = """
;W.number;W.title;W.short;W.month;W.year;W.dates;W.countryISO3;C.conference;C.title;C.countryISO3;C.start;C.end;C.timepoint;C.short;C.month;C.year # noqa: E501
0;76;29th Intl. Conf. ;VLDB 2003;9.0;2003;['2003', 'September', '2003'];DEU;http://www.wikidata.org/entity/Q113715819;29th International Conference on Very Large Data Bases;DEU;2003-09-09;2003-09-12 00:00:00;2003-09-09;VLDB 2003;9.0;2003.0 # noqa: E501
1;449;6th European Semantic Web Conference ;ESWC 2009;5.0;2009;['May 31, 2009'];GRC;http://www.wikidata.org/entity/Q64020447;ESWC 2009;GRC;2009-05-31;2009-06-04 00:00:00;2009-05-31;ESWC 2009;5.0;2009.0 # noqa: E501
2;464;6th European Semantic Web Conference ;ESWC 2009;6.0;2009;['2009', 'June 1st, 2009'];GRC;http://www.wikidata.org/entity/Q64020447;ESWC 2009;GRC;2009-05-31;2009-06-04 00:00:00;2009-05-31;ESWC 2009;5.0;2009.0 # noqa: E501
3;699;the European Semantic Web Conference 2010 ;ESWC 2010;5.0;2010;['2010'];GRC;http://www.wikidata.org/entity/Q64021290;ESWC 2010;GRC;2010-05-30;2010-06-03 00:00:00;2010-05-30;ESWC 2010;5.0;2010.0 # noqa: E501
4;1017;25th International Conference on Advanced Information Systems Engineering ;CAiSE 2013;6.0;2013;['2013'];ESP;http://www.wikidata.org/entity/Q106067743;Advanced Information Systems Engineering - 25th International Conference, CAiSE 2013, Valencia, Spain, June 17-21, 2013;ESP;2013-06-17;2013-06-21 00:00:00;2013-06-17;CAiSE 2013;6.0;2013.0 # noqa: E501
5;1018;International Conference on Very Large Databases ;VLDB 2013;8.0;2013;[];ITA;http://www.wikidata.org/entity/Q113715831;39th International Conference on Very Large Data Bases;ITA;2013-08-26;2013-08-30 00:00:00;2013-08-26;VLDB 2013;8.0;2013.0 # noqa: E501
6;1027;39th International Conference on Very Large Databases ;VLDB 2013;8.0;2013;[];ITA;http://www.wikidata.org/entity/Q113715831;39th International Conference on Very Large Data Bases;ITA;2013-08-26;2013-08-30 00:00:00;2013-08-26;VLDB 2013;8.0;2013.0 # noqa: E501
7;1026;9th International Conference on Semantic Systems ;I-SEMANTICS 2013;9.0;2013;[];AUT;http://www.wikidata.org/entity/Q106337563;I-SEMANTICS 2013 - 9th International Conference on Semantic Systems, ISEM '13, Graz, Austria, September 4-6, 2013;AUT;2013-09-04;2013-09-06 00:00:00;2013-09-04;I-SEMANTICS 2013;9.0;2013.0 # noqa: E501
8;1028;12th International Conference on Perspectives in Business Informatics Research ;BIR 2013;9.0;2013;[];POL;http://www.wikidata.org/entity/Q106079235;Perspectives in Business Informatics Research - 12th International Conference, BIR 2013, Warsaw, Poland, September 23-25, 2013;POL;2013-09-23;2013-09-25 00:00:00;2013-09-23;BIR 2013;9.0;2013.0 # noqa: E501
9;1030;12th International Semantic Web Conference ;ISWC 2013;10.0;2013;[];AUS;http://www.wikidata.org/entity/Q48025934;The 12th International Semantic Web Conference;AUS;2013-10-21;2013-10-25 00:00:00;2013-10-21;ISWC 2013;10.0;2013.0 # noqa: E501
10;1034;the 12th International Semantic Web Conference ;ISWC 2013;10.0;2013;[];AUS;http://www.wikidata.org/entity/Q48025934;The 12th International Semantic Web Conference;AUS;2013-10-21;2013-10-25 00:00:00;2013-10-21;ISWC 2013;10.0;2013.0 # noqa: E501
11;1045;12th International Semantic Web Conference ;ISWC 2013;10.0;2013;[];AUS;http://www.wikidata.org/entity/Q48025934;The 12th International Semantic Web Conference;AUS;2013-10-21;2013-10-25 00:00:00;2013-10-21;ISWC 2013;10.0;2013.0 # noqa: E501
12;1046;the International Semantic Web Conference ;ISWC 2013;10.0;2013;[];AUS;http://www.wikidata.org/entity/Q48025934;The 12th International Semantic Web Conference;AUS;2013-10-21;2013-10-25 00:00:00;2013-10-21;ISWC 2013;10.0;2013.0 # noqa: E501
"""

IN_CI = os.environ.get('CI', False)


@unittest.skipIf(IN_CI, "Skip in CI")
class TestNeo4j(unittest.TestCase):
    """
    test maintaining neo4j graph fro managing
    matching data
    """

    @classmethod
    def setUpClass(self):
        self.count_query = """
            match (n)
            return count(n) as count
            """
        self.graph = Graph()

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_init(self):
        """
        test whether the manager creates an empty neo4j server
        """
        # _ = Neo4jManager(password="ci")
        _ = Neo4jManager()

        num = self.graph.run(self.count_query)
        self.assertEqual(num.evaluate(), 0)

    def test_single_source(self):
        """
        test for a single matchsource, that nodes and relationships can be added
        adn that no duplicates will occur
        """
        # neo = Neo4jManager(password="ci")
        neo = Neo4jManager(delete_nodes=False)

        df = pd.read_csv(StringIO(test_data), sep=";")
        neo.add_matched_nodes(df, "number", "conference", "CeurWS", "Wikidata")

        num = self.graph.run(self.count_query)
        num = num.evaluate()

        # graph is not empty
        self.assertGreater(num, 0)

        # repeat insertions does not introduce duplicates
        neo.add_matched_nodes(df, "number", "conference", "CeurWS", "Wikidata")

        num2 = self.graph.run(self.count_query)
        num2 = num2.evaluate()

        self.assertEqual(num, num2)

    def test_wikidata_dblp_linking(self):
        """
        test linking between wikidata and dblp conferences
        """
        conferences = ["Q106087501", "Q106244990"]
        dblp_conferences = ["https://dblp.org/rec/conf/aecia/2014", "https://dblp.org/rec/conf/ict/2016"]

        matcher = Matcher()
        result = matcher.link_wikidata_dblp_conferences(conferences, "test", True)

        neo = Neo4jManager(delete_nodes=True)
        neo.add_matched_nodes_undirected(result, "conference", "dblp_id", "Wikidata", "Dblp",
                                         "conference", "conference", "linked")

        link_query = "match (n)-[:LINKED]->(c) return c.Dblp"
        conferences = self.graph.run(link_query).data()
        self.assertEqual(len(conferences), 4)
        conferences = [c["c.Dblp"] for c in conferences if c["c.Dblp"]]

        self.assertListEqual(dblp_conferences, conferences)

    def test_ceur_dblp_linking(self):
        """
        test linking Ceur-WS to dblp conferences
        """
        workshops = [
            {
                "number": 76,
                "title": "VLDB 2003 PhD Workshop"
            },
            {
                "acronym": "KRDB'94",
                "number": 1
            },
            {
                "acronym": "QPP++ 2023",
                "number": 3366
            }
        ]
        result = Matcher.link_workshops_dblp_conferences(workshops, reload=True)

        neo = Neo4jManager(delete_nodes=True)
        neo.add_matched_nodes(result,
                              key_w="number", key_c="conference_guess",
                              source_w="CeurWS", source_c="Dblp",
                              type_w="workshop", type_c="conference",
                              relation_type="linked")

        link_query = "match (n)-[:LINKED]->(c) return n"
        conferences = self.graph.run(link_query).data()
        self.assertGreater(len(conferences), 0)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
