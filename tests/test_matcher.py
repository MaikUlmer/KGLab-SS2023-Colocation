'''
Created on 2023-07-14

@author: nm
'''
import unittest
import os
import pandas as pd
from colocation.matcher import Matcher
from colocation.cache_manager import JsonCacheManager
from colocation.extractor import ColocationExtractor, ExtractionProcessor
from colocation.dataloaders.wikidata_loader import get_wikidata_conferences

IN_CI = os.environ.get('CI', False)


class TestMatcher(unittest.TestCase):
    """
    test main matching functionality of the module
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_fuzzy(self):
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

    def test_matcher(self):
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

    @unittest.skipIf(IN_CI, "Skip in CI environment")
    def test_workshop_dblp_linking(self):
        """
        test linking Ceur-WS to dblp conference proceedings
        """
        workshops = [
            {
                "stefan": 76,
                "title": "VLDB 2003 PhD Workshop"
            },
            {
                "acronym": "KRDB'94",
                "stefan": 1
            },
            {
                "acronym": "QPP++ 2023",
                "stefan": 3366
            }
        ]
        result = Matcher.link_workshops_dblp_conferences(workshops, "stefan")
        self.assertIsInstance(result, pd.DataFrame)
        self.assertTrue(result.shape[0] > 0,
                        msg="The linking produced not a single link.")

    @unittest.skipIf(IN_CI, "Skip in CI environment")
    def test_wikidata_dblp_linking(self):
        """
        test that wikidata conferences can be linked to dblp and that
        the Matcher object holds information that can be supplied to wikidata
        """
        conferences = ["Q106087501", "Q106244990", "Q861711", "Stefan"]
        expected = ["https://dblp.org/rec/conf/aecia/2014", "https://dblp.org/rec/conf/ict/2016"]

        matcher = Matcher()
        result = matcher.link_wikidata_dblp_conferences(conferences, "test", True)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertListEqual([r for r in result["dblp_id"].to_list() if r], expected,
                             msg=f"Expected results {expected} but got {[r for r in result['dblp_id'].to_list()]}.\
                               If this changes, check if the data has been supplemented in wikidata.")
        self.assertTrue(matcher.wikidata_supplement[
            ["dblp_event_supplement", "dblp_proceedings_supplement"]].any(axis=None))


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
