'''
Created on 2023-08-22

@author: nm
'''
import unittest
import os
from colocation.result_processor import ResultProcessor
from colocation.cache_manager import JsonCacheManager

IN_CI = os.environ.get('CI', False)

sure_colocations = [
    {
        "ceur": {
            "acronym": "VLDB 2003 PhD Workshop",
            "h1": "VLDB 2003 PhD Workshop",
            "Ceur-WS": 76,
            "Proceedings": "http://www.wikidata.org/entity/Q113546184",
            "Wikidata": [
                "https://test.wikidata.org/entity/Q232158"
            ],
        },
        "wikidata": {
            "month": 9.0,
            "year": 2003.0,
            "start": "2003-09-09",
            "short": "VLDB 2003",
            "end": "2003-09-12 00:00:00",
            "Wikidata": "https://test.wikidata.org/wiki/Q232159",
            "title": "29th International Conference on Very Large Data Bases",
            "countryISO3": "DEU",
            "timepoint": "2003-09-09"
        }
    }
]


class TestResultProcessor(unittest.TestCase):
    """
    test processing the matching results
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @classmethod
    def setUpClass(self):
        cacher = JsonCacheManager(base_url="", base_folder="results")
        cacher.store_lod("definite_result", sure_colocations, indent=True)

    def test_event_pairs(self):
        """
        test that the pairs are correctly extracted
        """
        data = [
            {
                "ceur": {"Wikidata": ["http://www.wikidata.org/entity/QHendrik",
                                      "http://www.wikidata.org/entity/QStefan"]},
                "wikidata": {"Wikidata": "http://www.wikidata.org/entity/QBlanc"}
            },
            {
                "ceur": {"Wikidata": ["http://www.wikidata.org/entity/Q2373489785"]},
                "wikidata": {"Wikidata": "http://www.wikidata.org/entity/Q1"}
            }
        ]

        cacher = JsonCacheManager(base_folder="results", base_url="")
        cacher.store_lod("test", data)

        processor = ResultProcessor('https://test.wikidata.org')
        res = processor.get_event_conference_pairs("test")

        self.assertEqual(len(res), 3,
                         msg="Incorrect number of pairs was produced.")
        self.assertListEqual([("Q", "Q") for _ in range(3)], [(left[0], right[0]) for left, right in res])

    def test_event_pairs_failing(self):
        """
        test that an empty list is returned, when the path is incorrect
        """
        processor = ResultProcessor('https://test.wikidata.org')
        res = processor.get_event_conference_pairs("trkjphor0hj590jh9045j049j55j")

        self.assertEqual(len(res), 0,
                         msg="Expected an empty list as the returned value.")

    @unittest.skipIf(IN_CI, "Skip in CI environment")
    def test_write_result_no_write(self):
        """
        test result write pipeline on above 'sure_colocations' without actually
        writing anything to wikidata.
        """
        processor = ResultProcessor('https://test.wikidata.org', write=False)

        res = processor.write_result_to_wikidata("definite_result")
        self.assertListEqual(res, ["Q232158"])

    @unittest.skipIf(IN_CI, "Skip in CI environment")
    def test_write_result(self):
        """
        test result write pipeline on above 'sure_colocations' without actually
        writing anything to wikidata.
        """
        processor = ResultProcessor('https://test.wikidata.org', write=True)

        res = processor.write_result_to_wikidata("definite_result")
        self.assertListEqual(res, ["Q232158"])


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
