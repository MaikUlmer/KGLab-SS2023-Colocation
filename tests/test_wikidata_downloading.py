'''
Created on 2023-06-28

@author: nm
'''
import unittest
import pandas as pd
from colocation.cache_manager import JsonCacheManager
from colocation.extractor import ColocationExtractor
from colocation.wikidata_loader import get_wikidata_conferences
from colocation.wikidata_loader import get_wikidata_workshops
from colocation.wikidata_loader import get_workshop_ids_from_lod


class TestMatcher(unittest.TestCase):
    """
    test download and caching
    """

    def setUp(self):
        self.skipTest("Skip in CI environment")

    def tearDown(self):
        pass

    def test_conference_loading(self):
        """
        test downloading conferences
        """
        conferences = get_wikidata_conferences(reload=True)

        self.assertTrue(conferences.any(axis=None))
        self.assertIsInstance(conferences, pd.DataFrame)
        self.assertTrue(conferences.shape[0] > 0)

        selected = ["conference", "conferenceLabel", "short", "countryISO3",
                    "start", "end", "timepoint"]
        self.assertSetEqual(set(selected), set(conferences.columns))

        pass

    def test_workshop_loading(self):
        """
        test downloading workshops
        """
        cacher = JsonCacheManager()
        volumes = cacher.load_lod("volumes")
        extractor = ColocationExtractor(volumes)

        extract = extractor.get_colocation_info()
        extract = [vol for vol in extract if vol["colocated"] is not None]
        ids = get_workshop_ids_from_lod(extract)

        self.assertTrue(ids)
        self.assertIsInstance(ids, list)
        self.assertTrue(len(ids) > 0)

        # ids look like wikidata ids
        for id in ids:
            self.assertEqual(id[0:4], "wd:Q")

        workshops = get_wikidata_workshops(ids, name="colocated", reload=True)

        self.assertTrue(workshops.any(axis=None))
        self.assertIsInstance(workshops, pd.DataFrame)
        self.assertTrue(workshops.shape[0] > 0)

        selected = ["workshop", "workshopLabel", "short", "countryISO3",
                    "locationLabel", "start", "end", "timepoint"]
        self.assertSetEqual(set(selected), set(workshops.columns))


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
