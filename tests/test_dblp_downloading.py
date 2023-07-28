'''
Created on 2023-07-28

@author: nm
'''
import unittest
import os
from pathlib import Path
from colocation.dataloaders.dblp_loader import DblpLoader


class TestDblp(unittest.TestCase):
    """
    test download and caching 
    """

    def setUp(self):
        self.skipTest("Skip in CI")
        self.loader = DblpLoader()
        self.base_path = f"{Path.home()}/.ceurws/dblp"

    def tearDown(self):
        pass

    def test_init(self):
        """
        test that downloader is setup correctly
        """
        self.assertTrue(self.loader)
        self.assertTrue(self.loader.use_cache)
        self.assertTrue(os.path.exists(self.base_path))

    def test_url_download(self):
        """
        test downloading a dblp page
        """
        url = "https://dblp.org/db/conf/icsr/index.html"
        page = self.loader.get_dblp_page(url)

        self.assertTrue(len(page) > 0)
        self.assertTrue(os.path.exists(f"{self.base_path}/{url}"))

    def test_conference_retrieval(self):
        """
        test that given a workshop, the corresponding conference is retrieved
        """
        short = "SMR2 2007"
        number = 243

        expected_result = ["https://dblp.org/db/conf/semweb/iswc2007.html"]

        res = self.loader.get_conferences_from_workshop(number, short)

        self.assertTrue(os.path.exists(f"{self.base_path}/https://dblp.org/db/series/ceurws/ceurws200-299.html"))
        self.assertTrue(os.path.exists(f"{self.base_path}/https://dblp.org/db/conf/semweb/index.html"))
        self.assertTrue(os.path.exists(f"{self.base_path}/https://dblp.org/db/conf/aswc/index.html"))

        self.assertListEqual(res, expected_result)

        self.assertTrue(number in self.loader.workshop_to_conference)
        self.assertTrue(expected_result[0] in self.loader.conference_doi)

    def test_uri_retrieval(self):
        short = "SMR2 2007"
        number = 243
        self.loader.get_conferences_from_workshop(number, short)

        id = "https://dblp.org/db/conf/semweb/iswc2007.html"
        expected = "https://doi.org/10.1007/978-3-540-76298-0"
        doi = self.loader.get_doi(id)

        self.assertEqual(expected, doi)


if __name__ == "__main__":
    unittest.main()
