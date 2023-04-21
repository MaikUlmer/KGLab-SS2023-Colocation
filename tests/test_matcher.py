'''
Created on 2023-04-21

@author: wf
'''
import unittest
from colocation.matcher import Matcher

class TestMatcher(unittest.TestCase):
    """
    test matching
    """


    def setUp(self):
        pass


    def tearDown(self):
        pass


    def testMatcher(self):
        """
        test matcher
        """
        matcher=Matcher()
        self.assertTrue(matcher is not None)
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()