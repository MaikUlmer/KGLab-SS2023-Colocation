'''
Created on 2023-04-28
@author: nm
'''
import urllib.request
import os
from pathlib import Path
import orjson

''''
mostly taken from https://github.com/ceurws/ceur-spt/blob/d7b5249a275179ca9aed4888f50ce31b927ec1f6/ceurspt/ceurws.py#L869
'''
class JsonCacheManager():
    """
    cache json based volume information
    """
    def __init__(self, base_url:str = "http://cvb.bitplan.com/volumes.json"):
        """
        constructor

        Args:
            base_url(str): url to json provider
        """ 
        self.base_url = base_url

    def json_path(self, lod_name:str)->str:
        """
        get path where lod with given name would be cached as json

        Args:
            lod_name(str): name of the list of dicts to get from cache
        
        Returns:
            str: the path to the lust of dicts cache
        """
        root_path = f"{Path.home()}/.ceurws"
        os.makedirs(root_path, exist_ok = True) # make directory if it does not exist
        json_path = f"{root_path}/{lod_name}.json"
        return json_path
    
    def load_lod(self, lod_name:str)->list:
        """
        load list of dicts from cache if possible and from url otherwise

        Args:
            lod_name(str): name of the list of dicts to get from cache
        
        Returns:
            list: the requested list of dicts
        """
        json_path=self.json_path(lod_name)
        if os.path.isfile(json_path):
            try:
                with open(json_path) as json_file:
                    json_str = json_file.read()
                    lod = orjson.loads(json_str)
            except Exception as e:
                msg=f"Could not read {lod_name} from {json_path} due to {str(e)}"
                raise Exception(msg)

        else:
            try:
                url = self.base_url
                with urllib.request.urlopen(url) as source:
                    json_str = source.read()
                    lod = orjson.loads(json_str)
            except Exception as e:
                msg=f"Could not read {lod_name} from source {url} due to {str(e)}"
                raise Exception(msg)
        return lod
    
    def store_lod(self, lod_name:str, lod:list):
        """
        stores list of dicts according to the given name

        Args:
            lod_name(str): name of the list of dicts
            lod(list): list of dicts to cache
        """
        store_path = self.json_path(lod_name)
        with open(store_path, 'wb') as json_file:
            json_str = orjson.dumps(lod)
            json_file.write(json_str)
            pass