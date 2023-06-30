'''
Created on 2023-05-10

@author: nm
'''
import unittest
from colocation.cache_manager import JsonCacheManager
from colocation.extractor import ColocationExtractor
from colocation.extractor import ExtractionProcessor
import pandas as pd

test_procs = [
        {
        "item": "http://www.wikidata.org/entity/Q113544519",
        "itemLabel": "Proceedings of the Second Workshop \"Automatische Bewertung von Programmieraufgaben\"",
        "itemDescription": "Proceedings of ABP 2015 workshop",
        "sVolume": 1496,
        "Volume": None,
        "short_name": "ABP 2015",
        "dblpProceedingsId": "conf/abp/2015",
        "title": "Proceedings of the Second Workshop \"Automatische Bewertung von Programmieraufgaben\"",
        "language_of_work_or_name": None,
        "language_of_work_or_nameLabel": None,
        "URN_NBN": "urn:nbn:de:0074-1496-0",
        "publication_date": "2015-10-31T00:00:00",
        "described_at_URL": "http://ceur-ws.org/Vol-1496/",
        "event": "http://www.wikidata.org/entity/Q113646167",
        "eventLabel": "Second Workshop \"Automatische Bewertung von Programmieraufgaben\"",
        "eventSeries": "",
        "eventSeriesLabel": "",
        "eventSeriesOrdinal": "",
        "dblpEventId": "conf/abp/abp2015",
        "fullWorkUrl": "http://ceur-ws.org/Vol-1496/",
        "ppnId": "848527844",
        "homePage": None
    },
    {
        "item": "http://www.wikidata.org/entity/Q113544494",
        "itemLabel": "Joint Proceedings of the 8th International Workshop on Model-based Architecting of Cyber-physical and Embedded Systems and 1st International Workshop on UML Consistency Rules",
        "itemDescription": "Proceedings of ACES-MB-WUCOR 2015 workshop",
        "sVolume": 1508,
        "Volume": None,
        "short_name": "ACES-MB-WUCOR 2015",
        "dblpProceedingsId": "conf/models/2015acesmb",
        "title": "Joint Proceedings of the 8th International Workshop on Model-based Architecting of Cyber-physical and Embedded Systems and 1st International Workshop on UML Consistency Rules",
        "language_of_work_or_name": None,
        "language_of_work_or_nameLabel": None,
        "URN_NBN": "urn:nbn:de:0074-1508-8",
        "publication_date": "2015-11-11T00:00:00",
        "described_at_URL": "http://ceur-ws.org/Vol-1508/",
        "event": "http://www.wikidata.org/entity/Q113638411|http://www.wikidata.org/entity/Q113638432",
        "eventLabel": "1st International Workshop on UML Consistency Rules|8th International Workshop on Model-based Architecting of Cyber-physical and Embedded Systems",
        "eventSeries": "",
        "eventSeriesLabel": "",
        "eventSeriesOrdinal": "",
        "dblpEventId": "conf/models/acesmb2015|conf/models/acesmb2015",
        "fullWorkUrl": "http://ceur-ws.org/Vol-1508/",
        "ppnId": "1029893004",
        "homePage": None
    },
    {
        "item": "http://www.wikidata.org/entity/Q113544493",
        "itemLabel": "Proceedings of 1st AI*IA Workshop on Intelligent Techniques At LIbraries and Archives",
        "itemDescription": "Proceedings of IT@LIA 2015 workshop",
        "sVolume": 1509,
        "Volume": None,
        "short_name": "IT@LIA 2015",
        "dblpProceedingsId": "conf/aiia/2015itlia",
        "title": "Proceedings of 1st AI*IA Workshop on Intelligent Techniques At LIbraries and Archives",
        "language_of_work_or_name": None,
        "language_of_work_or_nameLabel": None,
        "URN_NBN": "urn:nbn:de:0074-1509-2",
        "publication_date": "2015-11-12T00:00:00",
        "described_at_URL": "http://ceur-ws.org/Vol-1509/",
        "event": "http://www.wikidata.org/entity/Q113638412",
        "eventLabel": "1st AI*IA Workshop on Intelligent Techniques At LIbraries and Archives",
        "eventSeries": "",
        "eventSeriesLabel": "",
        "eventSeriesOrdinal": "",
        "dblpEventId": "conf/aiia/itlia2015",
        "fullWorkUrl": "http://ceur-ws.org/Vol-1509/",
        "ppnId": "1029898138",
        "homePage": None
    }
]

test_volumes = [
        {
        "fromLine": 47427,
        "toLine": 47450,
        "valid": 1,
        "url": "http://ceur-ws.org/Vol-1485/",
        "acronym": "AI*IA 2015 DC",
        "title": "Proceedings of the Doctoral Consortium (DC)",
        "loctime": "Ferrara, Italy, September 23-24, 2015",
        "tdtitle": "Proceedings of the Doctoral Consortium (AI*IA 2015 DC), Ferrara, Italy, September 23-24, 2015.",
        "volname": "AI*IA 2015 Doctoral Consortium",
        "editors": "Elena Bellodi, Alessio Bonfietti",
        "submittedBy": "Elena Bellodi",
        "published": "2015-10-23",
        "year": "2015",
        "pubDate": "2015-10-23T00:00:00",
        "number": 1485,
        "urn": "urn:nbn:de:0074-1485-8",
        "archive": "http://sunsite.informatik.rwth-aachen.de/ftp/pub/publications/CEUR-WS/Vol-1485.zip",
        "desc": "?",
        "h1": "AI*IA 2015 DC AI*IA 2015 Doctoral Consortium",
        "volume_number": "Vol-1485",
        "ceurpubdate": "2015-10-23",
        "voltitle": "AI*IA 2015 Doctoral Consortium",
        "homepage": "http://aixia2015.unife.it/events/doctoral-consortium-call-for-paper/",
        "h3": "Proceedings of the Doctoral Consortium (DC) co-located with the 14th Conference of the Italian Association for Artificial Intelligence (AI*IA 2015)",
        "colocated": "AI*IA 2015",
        "vol_number": None
    },
    {
        "fromLine": 47123,
        "toLine": 47146,
        "valid": 1,
        "url": "http://ceur-ws.org/Vol-1496/",
        "acronym": "ABP 2015",
        "title": "Proceedings of the Second Workshop \"Automatische Bewertung von Programmieraufgaben\"",
        "loctime": "Wolfenbüttel, Germany, November 6, 2015",
        "tdtitle": "Proceedings of the Second Workshop \"Automatische Bewertung von Programmieraufgaben\" (ABP 2015), Wolfenbüttel, Germany, November 6, 2015.",
        "volname": "Automatische Bewertung von Programmieraufgaben 2015",
        "editors": "Uta Priss, Michael Striewe",
        "submittedBy": "Uta Priss",
        "published": "2015-10-31",
        "year": "2015",
        "pubDate": "2015-10-31T00:00:00",
        "number": 1496,
        "urn": "urn:nbn:de:0074-1496-0",
        "archive": "http://sunsite.informatik.rwth-aachen.de/ftp/pub/publications/CEUR-WS/Vol-1496.zip",
        "desc": "?",
        "h1": "ABP 2015 Automatische Bewertung von Programmieraufgaben",
        "volume_number": "Vol-1496",
        "ceurpubdate": "2015-10-31",
        "voltitle": "Automatische Bewertung von Programmieraufgaben",
        "homepage": "http://ostfalia.de/cms/de/ecult/WorkshopABP2015.html",
        "h3": "Proceedings of the Second Workshop \"Automatische Bewertung von Programmieraufgaben\"",
        "colocated": None,
        "vol_number": None
    },
    {
        "fromLine": 46802,
        "toLine": 46831,
        "valid": 1,
        "url": "http://ceur-ws.org/Vol-1508/",
        "acronym": "ACES-MB-WUCOR 2015",
        "title": "Joint Proceedings of the 8th International Workshop on Model-based Architecting of Cyber-physical and Embedded Systems and 1st International Workshop on UML Consistency Rules",
        "loctime": "Ottawa, Canada, September 28, 2015",
        "tdtitle": "Joint Proceedings of the 8th International Workshop on Model-based Architecting of Cyber-physical and Embedded Systems and 1st International Workshop on UML Consistency Rules (ACES-MB-WUCOR 2015), Ottawa, Canada, September 28, 2015.",
        "volname": "Joint Proceedings of ACES-MB and WUCOR 2015",
        "editors": "Iulia Dragomir, Susanne Graf, Gabor Karsai, Florian Noyrit, Iulian Ober,",
        "submittedBy": "Iulia Dragomir",
        "published": "2015-11-11",
        "year": "2015",
        "pubDate": "2015-11-11T00:00:00",
        "number": 1508,
        "urn": "urn:nbn:de:0074-1508-4",
        "archive": "http://sunsite.informatik.rwth-aachen.de/ftp/pub/publications/CEUR-WS/Vol-1508.zip",
        "desc": "?",
        "h1": "ACES-MB-WUCOR 2015 Joint Proceedings of ACES-MB and WUCOR 2015",
        "volume_number": "Vol-1508",
        "ceurpubdate": "2015-11-11",
        "voltitle": "Joint Proceedings of ACES-MB and WUCOR 2015",
        "homepage": None,
        "h3": "Joint Proceedings of the 8th International Workshop on Model-based Architecting of Cyber-physical and Embedded Systems and 1st International Workshop on UML Consistency Rules (ACES-MB 2015 & WUCOR 2015) co-located with ACM/IEEE 18th International Conference on Model Driven Engineering Languages and Systems (MoDELS 2015)",
        "colocated": "MoDELS 2015",
        "vol_number": None
    },
    {
        "fromLine": 46776,
        "toLine": 46799,
        "valid": 1,
        "url": "http://ceur-ws.org/Vol-1509/",
        "acronym": "IT@LIA 2015",
        "title": "Proceedings of 1st AI*IA Workshop on Intelligent Techniques At LIbraries and Archives",
        "loctime": "Ferrara, Italy, September 22, 2015",
        "tdtitle": "Proceedings of 1st AI*IA Workshop on Intelligent Techniques At LIbraries and Archives (IT@LIA 2015), Ferrara, Italy, September 22, 2015.",
        "volname": "Intelligent Techniques At LIbraries and Archives 2015",
        "editors": "Stefano Ferilli, Nicola Ferro",
        "submittedBy": "Nicola Ferro",
        "published": "2015-11-12",
        "year": "2015",
        "pubDate": "2015-11-12T00:00:00",
        "number": 1509,
        "urn": "urn:nbn:de:0074-1509-2",
        "archive": "http://sunsite.informatik.rwth-aachen.de/ftp/pub/publications/CEUR-WS/Vol-1509.zip",
        "desc": "?",
        "h1": "IT@LIA 2015 Intelligent Techniques At LIbraries and Archives 2015",
        "volume_number": "Vol-1509",
        "ceurpubdate": "2015-11-12",
        "voltitle": "Intelligent Techniques At LIbraries and Archives 2015",
        "homepage": "http://italia2015.dei.unipd.it/",
        "h3": "Proceedings of 1st AI*IA Workshop on Intelligent Techniques At LIbraries and Archives co-located with XIV Conference of the Italian Association for Artificial Intelligence (AI*IA 2015)",
        "colocated": "AI*IA 2015",
        "vol_number": None
    }
]

vol1485 = {
    "version.version": "0.0.5",
    "version.cm_url": "https://github.com/ceurws/ceur-spt",
    "spt.html_url": "/Vol-1485.html",
    "spt.number": 1485,
    "spt.acronym": "AI*IA 2015 DC",
    "spt.wikidataid": None,
    "spt.title": "Proceedings of the Doctoral Consortium (DC)",
    "spt.description": None,
    "spt.url": None,
    "spt.date": "2015-10-23",
    "spt.dblp": None,
    "spt.k10plus": None,
    "spt.urn": None,
    "cvb.fromLine": 47286,
    "cvb.toLine": 47309,
    "cvb.valid": 1,
    "cvb.url": "http://ceur-ws.org/Vol-1485/",
    "cvb.acronym": "AI*IA 2015 DC",
    "cvb.title": "Proceedings of the Doctoral Consortium (DC)",
    "cvb.loctime": "Ferrara, Italy, September 23-24, 2015",
    "cvb.tdtitle": "Proceedings of the Doctoral Consortium (AI*IA 2015 DC), Ferrara, Italy, September 23-24, 2015.",
    "cvb.volname": "AI*IA 2015 Doctoral Consortium",
    "cvb.editors": "Elena Bellodi, Alessio Bonfietti",
    "cvb.submittedBy": "Elena Bellodi",
    "cvb.published": "2015-10-23",
    "cvb.year": "2015",
    "cvb.pubDate": "2015-10-23T00:00:00",
    "cvb.number": 1485,
    "cvb.urn": "urn:nbn:de:0074-1485-8",
    "cvb.archive": "http://sunsite.informatik.rwth-aachen.de/ftp/pub/publications/CEUR-WS/Vol-1485.zip",
    "cvb.desc": "?",
    "cvb.h1": "AI*IA 2015 DC AI*IA 2015 Doctoral Consortium",
    "cvb.volume_number": "Vol-1485",
    "cvb.ceurpubdate": "2015-10-23",
    "cvb.voltitle": "AI*IA 2015 Doctoral Consortium",
    "cvb.colocated": "AI*IA 2015",
    "cvb.homepage": "http://aixia2015.unife.it/events/doctoral-consortium-call-for-paper/",
    "cvb.h3": "Proceedings of the Doctoral Consortium (DC) co-located with the 14th Conference of the Italian Association for Artificial Intelligence (AI*IA 2015)",
    "cvb.vol_number": None
}


class DummyCacheManager(JsonCacheManager):
    """
    dummy cache manager to test the extractor
    """
    def __init__(self):
        pass

    def json_path(self, lod_name: str) -> str:
        pass

    def load_lod(self, lod_name: str) -> list:
        if lod_name == "Vol-1485":
            return vol1485
        return test_procs

    def store_lod(self, lod_name: str, lod: list):
        pass

    def reload_lod(self, lod_name: str) -> list:
        pass


class TestMatcher(unittest.TestCase):
    """
    test matching
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testExtractor(self):
        """
        test extractor on handpicked data
        """
        dummytester = DummyCacheManager()
        extractor = ColocationExtractor(test_volumes, dummytester, dummytester)
        self.assertTrue(extractor)
        self.assertTrue(len(extractor.get_colocation_info()) == 3)
        self.assertTrue(len(extractor.missing_events) == 1)

    def testLoctimeExtraction(self):
        """
        test that the Extraction processor correctly extracts from the provided lods
        """
        cacher = JsonCacheManager()
        volumes = cacher.load_lod("volumes")

        # check that the extractor generates colocation data
        extractor = ColocationExtractor(volumes)
        self.assertTrue(extractor)

        colocation_lod = extractor.get_colocation_info()
        self.assertTrue(colocation_lod)
        self.assertIsInstance(colocation_lod, list)
        self.assertTrue(len(colocation_lod) > 0)

        for extract in colocation_lod:
            self.assertTrue(extract)
            self.assertIsInstance(extract, dict)

        # check the data extraction
        processor = ExtractionProcessor(colocation_lod)
        self.assertTrue(processor)

        df = processor.get_loctime_info("colocated")
        self.assertIsInstance(df, pd.DataFrame)

        length = df.shape[0]

        # check if the dataframe has the correct columns
        colums = ["number", "colocated", "title", "short", "loctime",
                  "year", "month", "loc1", "loc2", "countryISO3"]
        self.assertSetEqual(set(colums), set(df.columns))

        # check if remove works
        processor.remove_events_by_keys(number_key="number", keys=[989])
        df = processor.get_loctime_info("colocated")

        self.assertEqual(df.shape[0] + 1, length)

        processor.remove_events_by_keys(number_key="number", keys=list(df["number"]))
        df = processor.get_loctime_info("colocated")

        self.assertEqual(df.shape[0], 0)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
