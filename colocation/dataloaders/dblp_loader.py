'''
Created on 2023-07-27
@author: nm
'''
import requests
import os
from pathlib import Path
from bs4 import BeautifulSoup as soup
from typing import Union, List


class DblpLoader():
    """
    Scrape data from Dblp and cache all results to reduce
    the number of requests sent.
    """

    def __init__(self, use_cache: bool = True):
        """
        Constructor.

        Args:
            use_cache(bool): sets initial behaviour if cache should be reused.
            Use set_cache_usage to change the value after initialisation.
        """
        self.use_cache = use_cache
        self.root_path = root_path = f"{Path.home()}/.ceurws/dblp"
        os.makedirs(root_path, exist_ok=True)

        self.conference_doi = {}          # map the Dblp id to the DOI
        self.workshop_to_conference = {}  # map CEUR-WS workshop found in conference series to conference

    def set_cache_usage(self, use_cache: bool):
        """
        Set whether the loader should use the cached files.

        Args:
            use_cache(bool): whether to reuse cache.
        """
        self.use_cache = use_cache

    def get_doi(self, dblp_id: str) -> Union[str, None]:
        """
        Takes the dblp id/ url of conference proceedings produced by this scraper
        and returns its doi if present

        Args:
            dblp_id(str): id/url of conference proceedings to get doi for.

        Returns:
            str|None: doi or None if not found
        """
        res = self.conference_doi[dblp_id] if dblp_id in self.conference_doi else None
        return res

    def get_dblp_page(self, url: str) -> str:
        """
        Method to get requested page from Dblp or internal cache.

        Args:
            url(str): url of the Dblp entity.

        Returns:
            str: content of the html file behind the url.
        """
        cached_path = f'{self.root_path}/{url}'
        if self.use_cache and os.path.isfile(cached_path):
            try:
                with open(cached_path, encoding="utf8") as f:
                    res = f.read()
            except Exception as e:
                msg = f"Could not read cached dblp file {url} due to {str(e)}."
                raise Exception(msg)

            return res

        try:
            r = requests.get(url)
        except Exception as e:
            msg = f"Dblp site request to {url} failed due to {str(e)}."

        if r.status_code != 200:
            msg = f"Dblp request to {url} returned error code {r.status_code}."
            raise Exception(msg)

        document = r.text
        try:
            with open(cached_path, "w", encoding="utf8") as f:
                f.write(document)
        except Exception as e:
            msg = f"Could not write dblp file from {url} to cache due to {str(e)}"
            raise Exception(msg)

    def get_conferences_from_workshop(self, vol_number: int, short: Union[str, None] = None) -> List[str]:
        """
        Get the Dblp ids of the conferences the workshop is co-located with according to
        the procedure link chasing.

        Args:
            vol_number(int): number of the CEUR-WS volume for the workshop.
            short(str|None): short title/acronym of the workshop to eliminate workshop series links.

        Returns:
            list(str): Dblp ids of the conferences the workshop is co-located with.
        """

        # check if we have already found the workshop previously
        if vol_number in self.workshop_to_conference:
            return self.workshop_to_conference[vol_number]

        # the conference series for CEUR-WS is split into ranges of length 100
        lower_bound = int(vol_number / 100) * 100
        subrange_url = f"https://dblp.org/db/series/ceurws/ceurws{lower_bound}-{lower_bound+99}.html"

        # find the link to the workshop in the range
        subrange_soup = soup(self.get_dblp_page(subrange_url), "html.parser")
        work_box = subrange_soup.find("div", class_="nr", string=vol_number).parent
        work_url = work_box.find("a", class_="toc-link")["href"]

        workshop_soup = soup(self.get_dblp_page(work_url), "html.parser")

        # get the links to potential conference links (container is named breadcrumbs)
        breadcrumbs = workshop_soup.find("div", id="breadcrumbs")
        conferences = breadcrumbs.find_all("meta", itemprop="position", content="3")
        conferences = [c.parent for c in conferences]
        conferences = [c.a["href"] for c in conferences]

        # delete links to workshop series
        series_names = [c.split("/")[-2] for c in conferences]
        conferences = [c for c, s in zip(conferences, series_names) if s not in short.lower()]

        # conferences now are the conference series
        # we now process these to extract as much information as possible
        res = []
        for conf_series in conferences:
            res.append(self.scrape_from_conference_series(conf_series=conf_series, vol_number=vol_number))

        return [r for r in res if r]

    def scrape_from_conference_series(self, conf_series: str, vol_number: int, year: int) -> str:
        """
        Infer as much data as possible from the given conference series about
        CEUR-WS workshops for later inference.

        Args:
            conf_series(str): url of the conference series
            vol_number(int): number of the CEUR-WS volume for the workshop.
            year(int): year of the workshop to identify conference

        Returns:
            str: Dblp id of the conferences the workshop is co-located with.
        """
        conference_soup = soup(self.get_dblp_page(conf_series, "html.parser"))

        # find all occurances of Ceur-WS workshop mentions
        ceur = conference_soup.find_all("a", href="https://dblp.org/db/series/ceurws/index.html")

        ceur_numbers = [int(c.next_sibling[1:-2]) for c in ceur]
        # go up to the list level and search for the link in the first item
        list_heads = [c.parent.parent.parent.next for c in ceur]
        parent_conferences = [c.find("a", itemprop="url")["href"] for c in list_heads]
        parent_conference_dois = [c.find("li", class_="ee").next["href"] for c in list_heads]
        # parent_conferences = [c.parent.parent.parent.next.find("a", class_="toc-link")["href"] for c in ceur]

        # add found relationship between volume and conference
        for num, conf in zip(ceur_numbers, parent_conferences):
            self.workshop_to_conference[num] = conf

        # get doi of the new conferences
        for conf, doi in zip(parent_conferences, parent_conference_dois):
            self.conference_doi[conf] = doi

        # now find the conference for the given workshop
        res: str
        if vol_number in self.workshop_to_conference:
            res = self.workshop_to_conference[vol_number]
        else:
            conference = conference_soup.find("h2", id=year)
            res = conference.parent.next_sibling.find("a", itemprop="url")["href"]

        return list(set(res))
