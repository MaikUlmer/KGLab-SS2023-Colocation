'''
Created on 2023-07-14

@author: nm
'''
import unittest
import pandas as pd
from colocation.matcher import Matcher


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
                "short": "VLDB",
                "title": "29th Intl. Conf. VLDB 2003, Berlin, Germany, September, 12-13, 2003.",
                "countryISO3": "GER",
                "year": 2003,
                "month": "September"
            },
            {
                "short": "STFN",
                "title": "10th international Conf. STFN 2004, Sydney, Australia",
                "countryISO3": "",
                "year": 2004,
                "month": ""
            }
        ]
        conference_lod = [
            {
                "short": "VLDB",
                "title": "29th international conference VLDB 2003, Berlin, Germany.",
                "countryISO3": "GER",
                "year": 2003,
                "month": ""
            },
            {
                "short": "VLDB",
                "title": "30th international conference VLDB 2005, Berlin, Germany.",
                "countryISO3": "GER",
                "year": 2005,
                "month": ""
            },
            {
                "short": "STFN",
                "title": "10th international Conf. STFN 2004, Sydney, Australia",
                "countryISO3": "",
                "year": 2004,
                "month": ""
            }
        ]

        workshops = pd.DataFrame(workshop_lod)
        conferences = pd.DataFrame(conference_lod)

        matcher = Matcher()
        res = matcher.fuzzy_title_matching(workshops, conferences, threshold=0.8)

        self.assertTrue(res.shape[0] == 1)
        self.assertTrue(str(res["C.title"].iloc[0]) == "29th international conference VLDB 2003, Berlin, Germany.")

    def testMatcher(self):
        """
        test matcher
        """
        matcher = Matcher()
        self.assertTrue(matcher is not None)
        pass


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
