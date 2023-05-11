from cache_manager import JsonCacheManager
import re


class ColocationExtractor():
    """
    Given a list of dicts, searches for "co-located" information. 
    """

    def __init__(self, volumes_lod:list,
                proc_provider:JsonCacheManager = JsonCacheManager(),
                extra_provider:JsonCacheManager = JsonCacheManager(base_url="http://ceurspt.wikidata.dbis.rwth-aachen.de")):
        """
        constructor
        Automatically extracts the information of the passed list of dicts.

        Args:
            volumes_lod(list): list of dict for the volumes to extract information from.
            proc_provider(JsonCacheManager): loader for additional volume information,
                should only be changed for test purposes.
            extra_provider(JsonCacheManager): loader for volume information not present in proc_provider,
                should only be changed for test purposes.
        """

        procs = "proceedings"

        self.extra_provider = extra_provider

        self.ceurWSProcs = proc_provider.load_lod(procs)


        self.volumes_lod = volumes_lod
        self.extract_info()


    def init_data_lods(self):
        """
        initializes 
        """


    def get_colocation_info(self):
        """
        Returns:
            list: the list of dicts containing the extracted colocation information
                for the relevant volumes.
        """
        return self.colocation_lod
    
    def find_wikidata_event(self):
        """
        Given extracted info for certain volumes, supplies these with
        their corresponding wikidata uri. 
        """
        colocation_lod = self.colocation_lod
        proceedings_map = {}
        missing_events = []

        for proc in self.ceurWSProcs:
            #Map volume number to wikidata uri for available volumes
            proceedings_map[proc["sVolume"]] = proc["event"]

        for volume in colocation_lod:
            if volume["number"] in proceedings_map.keys():
                uri = str(proceedings_map[volume["number"]]).split("|")
            else:
                vol_name = f'Vol-{volume["number"]}'
                vol = self.extra_provider.load_lod(vol_name)

                if "wd.event" in vol.keys():
                    uri = str(vol["wd.event"]).split("|")
                else:
                    uri = None
                    missing_events.append(volume)

            volume["wikidata_event"] = uri

        self.missing_events = missing_events

    def extract_info(self):
        """
        Extracts information from own volumes_lod and saves
        it within a list of dicts
        """
        colocation_lod = []
        regex = re.compile(
            "(co-located|colocated) with (?P<colocation>.*)"
        )
        at_regex = re.compile(
            "(\w*@.*)"
        )
        
        for volume in self.volumes_lod:
            volume_matches =[]
            volume_at_matches =[]
            
            for _, value in volume.items():
                result = re.search(regex, str(value))
                if(result is not None):
                    volume_matches.append(result["colocation"])

                at_result = re.search(at_regex, str(value))
                if(at_result is not None):
                    volume_at_matches.append(at_result[0])
            
            if volume["colocated"] or volume_matches or volume_at_matches:
                volume_dict = {}
                volume_number = int(volume["number"])
                volume_dict["number"] = volume_number
                volume_dict["colocated"] = volume["colocated"]
                volume_dict["extracted"] = volume_matches
                volume_dict["extracted@"] = volume_at_matches

                colocation_lod.append(volume_dict)

        self.colocation_lod=colocation_lod
        self.find_wikidata_event()
