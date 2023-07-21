'''
Created on 2023-07-14

@author: nm
'''
import unittest
import pandas as pd
from colocation.matcher import Matcher
from colocation.cache_manager import JsonCacheManager
from colocation.extractor import ColocationExtractor, ExtractionProcessor
from colocation.wikidata_loader import get_wikidata_conferences


class TestMatcher(unittest.TestCase):
    """
    test main matching functionality of the module
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testFuzzy(self):
        """
        test that titles can be matched fuzzily
        and that only ones with the same year are matched
        """
        workshop_lod = [
            {
                "W.short": "VLDB",
                "W.title": "29th Intl. Conf. VLDB 2003, Berlin, Germany, September, 12-13, 2003.",
                "W.countryISO3": "GER",
                "W.year": 2003,
                "W.month": "September"
            },
            {
                "W.short": "STFN",
                "W.title": "10th international Conf. STFN 2004, Sydney, Australia",
                "W.countryISO3": "None",
                "W.year": 2004,
                "W.month": None
            }
        ]
        conference_lod = [
            {
                "C.short": "VLDB",
                "C.title": "29th international conference VLDB 2003, Berlin, Germany.",
                "C.countryISO3": "GER",
                "C.year": 2003,
                "C.month": ""
            },
            {
                "C.short": "VLDB",
                "C.title": "30th international conference VLDB 2005, Berlin, Germany.",
                "C.countryISO3": "GER",
                "C.year": 2005,
                "C.month": ""
            },
            {
                "C.short": "STFN",
                "C.title": "10th international Conf. STFN 2004, Sydney, Australia",
                "C.countryISO3": "None",
                "C.year": 2004,
                "C.month": None
            }
        ]

        workshops = pd.DataFrame(workshop_lod)
        conferences = pd.DataFrame(conference_lod)

        matcher = Matcher()
        res = matcher.fuzzy_title_matching(workshops, conferences, threshold=0.6)
        self.assertTrue(res.shape[0] == 1)
        self.assertTrue(str(res["C.title"].iloc[0]) == "29th international conference VLDB 2003, Berlin, Germany.")

    def testMatcher(self):
        """
        test matcher on two smaller keywords
        """
        cacher = JsonCacheManager()
        volumes = cacher.load_lod("volumes")

        extractor = ColocationExtractor(volumes)
        colocation_lod = extractor.get_colocation_info()

        processor = ExtractionProcessor(colocation_lod)

        conferences = get_wikidata_conferences()
        matcher = Matcher(types_to_match=["hosted", "aff"])

        res = matcher.match_extract(
            processor.get_loctime_info,
            processor.remove_events_by_keys,
            "number",
            conferences,
            0.7
        )

        self.assertIsInstance(res, pd.DataFrame)
        self.assertTrue(res.shape[0] > 0)
        self.assertTrue(res.shape[0] < 654)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
