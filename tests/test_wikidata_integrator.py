'''
Created on 2023-08-24

@author: nm
'''
import unittest
import os
from colocation.wikidata_integrator import WikidataWriter

IN_CI = os.environ.get('CI', False)


@unittest.skipIf(IN_CI, "Skip in CI environment")
class TestResultProcessor(unittest.TestCase):
    """
    test Wikidata Write authentication for Wikidata test instance
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_credentials_present(self):
        """
        test that credentials are actually present
        """
        wbi = WikidataWriter('https://test.wikidata.org')
        user, pwd = wbi.getCredentials()

        self.assertTrue(user)
        self.assertTrue(pwd)

    def test_login(self):
        """
        test that the user can sucessfully login using their credentials
        """
        wbi = WikidataWriter('https://test.wikidata.org')

        wbi.loginWithCredentials()


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
