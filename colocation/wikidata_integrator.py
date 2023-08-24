'''
Created on 2023-04-28
@author: nm

Base framework taken from
https://github.com/WolfgangFahl/PyGenericSpreadSheet/blob/84496d0832186fcf896c8f94fb51368690a1cbaa/spreadsheet/wikidata.py#L94
'''
from wikibaseintegrator import WikibaseIntegrator, wbi_login
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.datatypes import Item
from .values import Bot
from typing import Literal, Optional, List, Tuple
import os
import orjson
from pathlib import Path


class WikidataWriter():
    """
    Integrator for writing the co-located attribute into Wikidata using
    the Wikimedia api provided by Wikibase integrator.
    """

    TEST_WD_URL = "https://test.wikidata.org"
    WD_URL = "https://www.wikidata.org"

    TEST_PROPERTY = "P348"
    WD_PROPERTY = "P11633"  # co-located

    def __init__(self, baseurl: Optional[Literal["https://www.wikidata.org/", "https://test.wikidata.org"]],
                 write: bool = False):
        '''
        Constructor

        Args:
            baseurl(str): the baseurl of the wikibase to use
            debug(bool): if True output debug information
            write(bool): if true, actually performs the write
        '''
        if baseurl is None:
            baseurl = self.WD_URL
        self.baseurl = baseurl
        self.property = self.WD_PROPERTY if baseurl == self.WD_URL else self.TEST_PROPERTY
        self.write = write
        self.apiurl = f"{self.baseurl}/w/api.php"
        self.login = None
        self.user = None
        self._wbi = None

    @property
    def wbi(self) -> WikibaseIntegrator:
        """
        Getter for WikibaseIntegrator
        """
        if self._wbi is None or (self.login is not None and self._wbi.login is None):
            wbi_config['USER_AGENT'] = f'{Bot.name}/{Bot.version} (https://www.wikidata.org/wiki/User:{self.user})'
            wbi_config['MEDIAWIKI_API_URL'] = self.apiurl
            self._wbi = WikibaseIntegrator(login=self.login)
        return self._wbi

    @wbi.setter
    def wbi(self, wbi: Optional[WikibaseIntegrator]):
        """
        set the WikibaseIntegrator
        """
        self._wbi = wbi

    def getCredentials(self) -> (str, str):
        """
        get my credentials https://test.wikidata.org/wiki/Property:P370

        from the wd npm command line tool

        Throws:
            Exception: if no credentials are available for the baseurl

        Returns:
            (username, password) of the account assigned to the baseurl
        """
        user = None
        pwd = None
        home = str(Path.home())
        configFilePath = f"{home}/.config/wikibase-cli/config.json"
        if os.path.isfile(configFilePath):
            with open(configFilePath, mode="r") as f:
                json_str = f.read()
                wikibaseConfigJson = orjson.loads(json_str)
                credentials = wikibaseConfigJson["credentials"]
                credentialRecord = credentials.get(self.baseurl, None)
                if self.baseurl == self.TEST_WD_URL and self.baseurl not in credentials and self.WD_URL in credentials:
                    credentialRecord = credentials.get(self.WD_URL)
                if credentialRecord is None:
                    raise Exception(f"no credentials available for {self.baseurl}")
                user = credentialRecord["username"]
                pwd = credentialRecord["password"]
        return user, pwd

    def loginWithCredentials(self, user: Optional[str] = None, pwd: Optional[str] = None):
        """
        login using the given credentials or credentials
        retrieved via self.getCredentials

        Args:
            user(str): the username
            pwd(str): the password
        """
        if user is None:
            user, pwd = self.getCredentials()

        if user is not None:
            self.login = wbi_login.Login(user=user, password=pwd, mediawiki_api_url=self.apiurl)
            if self.login:
                self.user = user

    def write_colocated_attributes(self, result_pairs: List[Tuple[str, str]]) -> List[str]:
        """
        set the co-located attribute for each left item in the list of tuples to the
        corresponding right item.

        Args:
            result_pairs(list(str, str)): list of co-located workshop conference pairs

        Returns:
            list(str): list of workshop item ids for whom the co-located attribute was written
        """

        wbi = self.wbi
        res = []

        for workshop, conference in result_pairs:
            workshop_item = wbi.item.get(workshop)
            workshop_item.claims.add(Item(prop_nr=self.property, value=conference))  # co-located with

            if self.write:
                workshop_item = workshop_item.write()

            res.append(workshop_item.id)

        return res
