'''
Created on 2023-04-28

@author: nm
'''
import unittest
import os
from pathlib import Path
from colocation.cache_manager import JsonCacheManager


class TestMatcher(unittest.TestCase):
    """
    test download and caching Ceur-WS
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testCacher(self):
        """
        test download and chacher
        """
        cacher = JsonCacheManager()
        self.assertTrue(cacher is not None)
        self.assertEqual(cacher.base_url, "http://cvb.bitplan.com")

        lod_name = "volumes"
        lod = cacher.load_lod(lod_name)
        self.assertTrue(lod)

        cacher.store_lod(lod_name, lod)
        self.assertTrue(os.path.isfile(f"{Path.home()}/.ceurws/volumes.json"))

        pass


if __name__ == "__main__":
    unittest.main()
