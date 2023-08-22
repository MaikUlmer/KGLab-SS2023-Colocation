'''
Created on 2023-08-22

@author: nm
'''
import unittest
from pathlib import Path
import orjson
from colocation.result_processor import ResultProcessor


class TestResultProcessor(unittest.TestCase):
    """
    test processing the matching results
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_event_pairs(self):
        """
        test that the pairs are correctly extracted
        """
        store_path = f"{Path.home()}/.ceurws/results/test.json"

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
        with open(store_path, 'wb') as json_file:
            json_string = orjson.dumps(data)
            json_file.write(json_string)

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


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
