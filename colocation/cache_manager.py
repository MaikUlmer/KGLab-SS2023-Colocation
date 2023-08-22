'''
Created on 2023-04-28
@author: nm
'''
import urllib.request
import os
from pathlib import Path
import orjson
import pandas as pd
from typing import List, Dict, Union


# mostly from https://github.com/ceurws/ceur-spt/blob/d7b5249a275179ca9aed4888f50ce31b927ec1f6/ceurspt/ceurws.py#L869

class JsonCacheManager():
    """
    cache json based volume information
    """
    def __init__(self, base_url: str = "http://cvb.bitplan.com", base_folder: Union[str, None] = None):
        """
        constructor

        Args:
            base_url(str): base url of json provider
            base_folder(str|None): folder to put cached files into
        """
        self.base_url = base_url
        self.base_folder = base_folder

    def json_path(self, lod_name: str) -> str:
        """
        get path where lod with given name would be cached as json

        Args:
            lod_name(str): name of the list of dicts to get from cache

        Returns:
            str: the path to the lust of dicts cache
        """
        root_path = f"{Path.home()}/.ceurws"
        if self.base_folder:
            root_path += f"/{self.base_folder}"
        os.makedirs(root_path, exist_ok=True)  # make directory if it does not exist
        json_path = f"{root_path}/{lod_name}.json"
        return json_path

    def load_lod(self, lod_name: str) -> List[Dict]:
        """
        load list of dicts from cache if possible and from url otherwise

        Args:
            lod_name(str): name of the list of dicts to get from cache

        Returns:
            list(dict): the requested list of dicts
        """
        json_path = self.json_path(lod_name)
        if os.path.isfile(json_path):
            try:
                with open(json_path, encoding="utf8") as json_file:
                    json_str = json_file.read()
                    lod = orjson.loads(json_str)
            except Exception as e:
                msg = f"Could not read {lod_name} from {json_path} due to {str(e)}."
                raise Exception(msg)

        else:
            lod = self.reload_lod(lod_name)
        return lod

    def store_lod(self, lod_name: str, lod: List[Dict], indent: bool = False):
        """
        stores list of dicts according to the given name

        Args:
            lod_name(str): name of the list of dicts
            lod(list(dict)): list of dicts to cache
            indent(bool): whether to format the json file to be readable
        """
        store_path = self.json_path(lod_name)
        with open(store_path, 'wb') as json_file:
            json_str = orjson.dumps(lod) if not indent else orjson.dumps(lod, option=orjson.OPT_INDENT_2)
            json_file.write(json_str)
            pass

    def reload_lod(self, lod_name: str) -> List[Dict]:
        """
        forces load from url and may overwrite local copy

        Args:
            lod_name(str): name of the list of dicts to reload

        Returns:
            list: the reloaded list of dicts
        """
        try:
            url = f'{self.base_url}/{lod_name}.json'
            with urllib.request.urlopen(url) as source:
                json_str = source.read()
                lod = orjson.loads(json_str)
        except Exception as e:
            msg = f"Could not read {lod_name} from source {url} due to {str(e)}."
            raise Exception(msg)

        self.store_lod(lod_name, lod)
        return lod


class CsvCacheManager():
    """
    cache pandas dataframe based information as csv
    """
    def __init__(self, base_folder: Union[str, None] = None):
        """
        constructor

        Args:
            base_folder(str|None): folder to put cached files into
        """
        self.base_folder = base_folder

    def save_path(self, df_name: str) -> str:
        """
        get path where dataframe with given name would be cached as csv

        Args:
            lod_name(str): name of the dataframe to get from cache

        Returns:
            str: the path to the lust of dicts cache
        """
        root_path = f"{Path.home()}/.ceurws"
        if self.base_folder:
            root_path += f"/{self.base_folder}"
        os.makedirs(root_path, exist_ok=True)  # make directory if it does not exist
        csv_path = f"{root_path}/{df_name}.csv"
        return csv_path

    def load_csv(self, df_name: str) -> Union[pd.DataFrame, None]:
        """
        load pandas DataFrmae from cache if possible

        Args:
            df_name(str): name of the dataframe to get from cache

        Returns:
            pandas.DataFrame|None: the requested dataframe or None
        """
        csv_path = self.save_path(df_name)
        if os.path.isfile(csv_path):
            try:
                df = pd.read_csv(csv_path)
            except Exception as e:
                msg = f"Could not read {df_name} from {csv_path} due to {str(e)}."
                raise Exception(msg)
        else:
            df = None

        return df

    def store_csv(self, csv_name: str, df: pd.DataFrame):
        """
        stores list of dicts according to the given name

        Args:
            csv_name(str): name of the csv file
            df(pandas.DataFrame): dataframe to cache
        """
        store_path = self.save_path(csv_name)
        df.to_csv(store_path)
