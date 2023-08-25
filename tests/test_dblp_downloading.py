'''
Created on 2023-08-02

@author: nm
'''
import unittest
import os
from pathlib import Path
import pandas as pd
import numpy as np
from colocation.dataloaders.dblp_loader import (get_dblp_conferences, get_dblp_workshops, guess_dblp_conference,
                                                dblp_proceedings_to_events, dblp_events_to_proceedings,
                                                verify_dblp_uris, verify_dblp_events)


IN_CI = os.environ.get('CI', False)


class TestDblp(unittest.TestCase):
    """
    test download and caching
    """

    @unittest.skipIf(IN_CI, "Skip in CI environment")
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_conference_guess(self):
        """
        test guessing the uri for the co-located conference for a given workshop
        """
        workshop = "https://dblp.org/rec/conf/agile/2013phd"
        expected = "https://dblp.org/rec/conf/agile/2013"

        res = guess_dblp_conference(pd.DataFrame([{"volume": workshop}]))
        self.assertTrue(res.shape[0] == 1)
        self.assertIsInstance(res, pd.DataFrame)
        self.assertEqual(res["conference_guess"].iloc[0], expected)

    def test_conference_loading(self):
        """
        test downloading conferences
        """
        conferences = get_dblp_conferences(reload=True)
        path = f"{Path.home()}/.ceurws/dblp_conferences.csv"

        self.assertTrue(os.path.isfile(path),
                        msg=f"Could not save Dblp conferences to csv at {path}.")

        self.assertIsInstance(conferences, pd.DataFrame)
        self.assertTrue(conferences.shape[0] > 0)

    def test_workshop_loading(self):
        """
        test downloading workshops
        """
        volumes = [i for i in range(1000, 1002)]
        path = f"{Path.home()}/.ceurws/dblp/workshops-1000-1001.csv"

        workshops = get_dblp_workshops(volumes, name="test-volumes", reload=True)

        self.assertTrue(os.path.isfile(path),
                        msg=f"Could not save Dblp workshops to csv at {path}.")

        self.assertIsInstance(workshops, pd.DataFrame)
        self.assertTrue(workshops.shape[0] >= 2)

        self.assertTrue("conference_guess" in workshops.columns)

    def test_proceedings_to_events(self):
        """
        test conversion of proceedings id to event id
        """
        proceedings = ["https://dblp.org/rec/conf/aaai/2019", "https://dblp.org/rec/conf/agile/2013",
                       None, "Stefan"]
        expected = ["https://dblp.org/db/conf/aaai/aaai2019", "https://dblp.org/db/conf/agile/agile2013",
                    np.nan, np.nan]

        procs = pd.Series(data=proceedings)
        result = dblp_proceedings_to_events(procs)

        self.assertTrue(result.shape[0] > 0)

        events = result.to_list()

        self.assertListEqual(expected, events,
                             msg=f"Expected {expected} but got {events}.")

    def test_events_to_proceeedings(self):
        """
        test conversion ofevent id to proceeding id
        """
        events = ["https://dblp.org/db/conf/aaai/aaai2019", "https://dblp.org/db/conf/agile/agile2013", None, np.nan]
        expected = ["https://dblp.org/rec/conf/aaai/2019", "https://dblp.org/rec/conf/agile/2013", None, np.nan]

        evs = pd.Series(data=events)
        result = dblp_events_to_proceedings(evs)

        self.assertTrue(result.shape[0] > 0)
        proceedings = result.to_list()

        self.assertListEqual(expected, proceedings,
                             msg=f"Expected {expected} but got {proceedings}.")

    def test_uri_verification(self):
        """
        test querying for the existance of dblp uris
        """
        uris = ["https://dblp.org/rec/conf/aaai/2019", "https://dblp.org/rec/conf/agile/2013",
                "https://dblp.org/rec/conf/aaai/a2019"]
        expected = [True, True, False]

        uri_column = pd.Series(data=uris)
        result = verify_dblp_uris(uri_column)
        self.assertIsInstance(result, pd.Series)
        self.assertListEqual(expected, result.to_list())

    def test_event_verification(self):
        """
        test querying for the connection between dblp proceeding uris and event url
        """
        uris = ["https://dblp.org/rec/conf/aaai/2019", "https://dblp.org/rec/conf/agile/2013",
                "https://dblp.org/rec/conf/aaai/2018", "stefan", "https://dblp.org/rec/conf/aaai/2016", None]
        urls = ["https://dblp.org/db/conf/aaai/aaai2019", "https://dblp.org/db/conf/agile/agile2013",
                "https://dblp.org/db/conf/aaai/aaai2019", "stefan", None, None]
        expected = [True, True, False, False, False, False]

        uri_column = pd.Series(data=uris)
        url_column = pd.Series(data=urls)

        result = verify_dblp_events(uri_column, url_column)
        self.assertIsInstance(result, pd.Series)
        self.assertListEqual(expected, result.to_list())

    def test_converter_empty_input(self):
        """
        test whether conversion between event and proceedings works on empty inputs
        """
        input = pd.Series(data=[])
        eve_to_proc = dblp_events_to_proceedings(input)
        proc_to_eve = dblp_events_to_proceedings(input)

        self.assertTrue(not eve_to_proc.any())
        self.assertTrue(not proc_to_eve.any())


if __name__ == "__main__":
    unittest.main()
