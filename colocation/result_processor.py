'''
Created on 2023-08-22
@author: nm

Handles different types of results (TODO) and manages result import into wikidata.
'''

from typing import Literal, List, Tuple
from .cache_manager import JsonCacheManager


class ResultProcessor():
    """
    Class to handle different results as produced by the neo4j database
    and input surefire cases into wikidata.
    """

    def __init__(self, wikibase_instance: Literal["https://www.wikidata.org/", "https://test.wikidata.org"]):
        """
        Constructor.

        Args:
            wikibase_instance(str): wikibase instance to add items to, whether it be the real one or one for testing.
        """
        self.wikibase_instance = wikibase_instance
        self.result_loader = JsonCacheManager(base_url="", base_folder="results")

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
