'''
Created on 2023-08-22
@author: nm

Handles different types of results (TODO) and manages result import into wikidata.
'''

from typing import Literal, List, Tuple
from .cache_manager import JsonCacheManager
from .wikidata_integrator import WikidataWriter


class ResultProcessor():
    """
    Class to handle different results as produced by the neo4j database
    and input surefire cases into wikidata.
    """

    def __init__(self,
                 wikibase_instance: Literal["https://www.wikidata.org/", "https://test.wikidata.org"],
                 write: bool = False):
        """
        Constructor.

        Args:
            wikibase_instance(str): wikibase instance to add items to, whether it be the real one or one for testing.
            write(bool): if yes, actually write the result to the wikibase instance given.
        """
        self.wikibase_instance = wikibase_instance
        self.result_loader = JsonCacheManager(base_url="", base_folder="results")
        self.write = write

    def get_event_conference_pairs(self, result_name: str) -> List[Tuple[str, str]]:
        """
        Gets the name of json file of the desired result and returns the list consisting of the
        pairs of Ceur-WS wikidata entities and the co-located conference wikidata entities.

        Args:
            result_name(str): name of the json file to get the co-location pairs from.
        Returns:
            list((str, str)): pairs ceur:conference of co-located wikidata events
        """
        try:
            lod = self.result_loader.load_lod(result_name)
        except Exception:
            print(f"The requested result json file {result_name} is not present in the results folder.")
            return []

        res = [
            (ceur.split("/")[-1], volume["wikidata"]["Wikidata"].split("/")[-1])
            for volume in lod for ceur in volume["ceur"]["Wikidata"]
        ]
        return res

    def write_result_to_wikidata(self, result_name: str) -> List[str]:
        """
        Write the co-located attribute for the workshop conference pairs into Wikidata as is
        given by the specified result json file.

        Args:
            result_name(str): name of the json file to get the co-location pairs from.

        Returns:
            list(str): list of workshop item ids for whom the co-located attribute was written.
        """
        result_pairs = self.get_event_conference_pairs(result_name=result_name)
        wbi = WikidataWriter(baseurl=self.wikibase_instance, write=self.write)

        wbi.loginWithCredentials()
        written = wbi.write_colocated_attributes(result_pairs=result_pairs)

        return written
