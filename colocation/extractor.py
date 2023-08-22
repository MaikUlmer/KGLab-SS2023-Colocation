from .cache_manager import JsonCacheManager
import re
import pandas as pd
import spacy
import country_converter as coco
from typing import List, Dict


matchtypes = ["coloc", "hosted", "aff", "conjunction", "@2", "part", "affiliated", "at"]
matchregexes = {}

matchregexes[matchtypes[0]] = re.compile(
    "(?:(?:co-located|colocated|collocated) with) (.*)"
)
matchregexes[matchtypes[1]] = re.compile(
    "(?:hosted by )(.*)"
)
matchregexes[matchtypes[2]] = re.compile(
    "(?:affiliated with )(.*)"
)
matchregexes[matchtypes[3]] = re.compile(
    "(?:in conjunction with )(.*)"
)
matchregexes[matchtypes[4]] = re.compile(
    "(?:\w* @ )(.*)"
)
matchregexes[matchtypes[5]] = re.compile(
    "(?:part of )(.*)"
)
matchregexes[matchtypes[6]] = re.compile(
    "(?:affiliated (?:with|to) )(.*)"
)
matchregexes[matchtypes[7]] = re.compile(
    "(?:\w* at )(.*)"
)


class ColocationExtractor():
    """
    Given a list of dicts, searches for "co-located" information.
    """

    def __init__(self, volumes_lod: List[Dict],
                 proc_provider: JsonCacheManager = JsonCacheManager(),
                 extra_provider: JsonCacheManager =
                 JsonCacheManager(base_url="http://ceurspt.wikidata.dbis.rwth-aachen.de")):
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
        self.matchtypes = matchtypes

        procs = "proceedings"

        self.extra_provider = extra_provider

        self.ceurWSProcs = proc_provider.load_lod(procs)

        self.volumes_lod = volumes_lod
        self.extract_info()

    def get_colocation_info(self) -> List[Dict]:
        """
        Returns:
            list: the list of dicts containing the extracted colocation information
                for the relevant volumes.
        """
        return self.colocation_lod

    def get_colocation_volume_numbers(self) -> List[int]:
        """
        Returns:
            list: the list of dicts containing the volume numbers of all volumes
                  for which this instance has extracted information.
        """
        return [workshop["number"] for workshop in self.colocation_lod]

    def find_wikidata_event(self):
        """
        Given extracted info for certain volumes, supplies these with
        their corresponding wikidata uri.
        """
        colocation_lod = self.colocation_lod
        missing_events = []

        # Map volume number to wikidata uri for available volumes
        event_map = {proc["sVolume"]: proc["event"] for proc in self.ceurWSProcs if proc["event"]}
        proceedings_map = {proc["sVolume"]: proc["item"] for proc in self.ceurWSProcs}

        for volume in colocation_lod:
            if volume["number"] in event_map.keys():
                uri = str(event_map[volume["number"]]).split("|")
            else:
                vol_name = f'Vol-{volume["number"]}'
                vol = self.extra_provider.load_lod(vol_name)

                if "wd.event" in vol.keys() and vol["wd.event"]:
                    uri = str(vol["wd.event"]).split("|")
                else:
                    uri = None
                    missing_events.append(volume)

            volume["wikidata_event"] = uri
            volume["wikidata_proceedings"] = (
                proceedings_map[volume["number"]] if volume["number"] in proceedings_map else "")

        self.missing_events = missing_events

    def extract_info(self):
        """
        Extracts information from own volumes_lod and saves
        it within a list of dicts
        """
        colocation_lod = []

        for volume in self.volumes_lod:
            matches = {mt: [] for mt in matchtypes}

            for _, value in volume.items():

                # use appropriate regex for each search type
                for mt in matchtypes:
                    result = re.search(matchregexes[mt], str(value))
                    if result is not None:
                        matches[mt].append(result[1])

            # check for any posssible match
            if volume["colocated"] or any([m for m in matches.values()]):
                volume_dict = {}
                volume_number = int(volume["number"])
                volume_dict["number"] = volume_number
                volume_dict["colocated"] = volume["colocated"]
                volume_dict["loctime"] = volume["loctime"]
                volume_dict["acronym"] = volume["acronym"]

                for mt in matchtypes:
                    volume_dict[mt] = matches[mt]

                colocation_lod.append(volume_dict)

        self.colocation_lod = colocation_lod
        self.find_wikidata_event()


class ExtractionProcessor():
    """
    Given extracted information about events in a lod,
    processes the extract to generate attributes like the (short) title,
    time and place of the conference in question to use in incremental matching.
    """

    def __init__(self, extract_lod: List[Dict]):
        """"
        constructor
        initialises the events from which additional information is required
        Args:
            extract_lod(list(dict)): extract of the events of interest
        """
        # we need a shallow copy to reuse the lod elsewhere
        self.remaining_events = extract_lod.copy()

        self.nlp = spacy.load("en_core_web_sm")
        self.year_regex = re.compile("[0-9]{4}")
        self.months = ["january", "february", "march,", "april", "may", "june",
                       "july", "august", "september", "october", "november", "december"]
        self.month_regex = re.compile(
            "|".join(self.months), re.IGNORECASE)

        self.month_numerizer = dict((v, k) for v, k in zip(self.months, range(1, 13)))

    # this does not work because the dataframe indices are different from the overall lod
    # def remove_events_by_index(self, indices: list):
    #     """
    #     Given a list of indices, removes the corresponding events from the
    #     internal remaining events lod
    #     Args:
    #         indices(list[int]): indices of events to remove
    #     """
    #     for index in sorted(indices, reverse=True):
    #         del self.remaining_events[index]

    def remove_events_by_keys(self, number_key: str, keys: List[int]):
        """
        Given a list of numbers, removes the events with corresponding number from the
        internal remaining events lod.
        Args:
            number_key(str): name of the key in the extract lod to match number against
            keys(list[int]): indices of events to remove
        """

        # iterate in reverse order over list indices
        for index in range(len(self.remaining_events) - 1, -1, -1):
            if self.remaining_events[index][number_key] in keys:
                del self.remaining_events[index]

    def split_by_short_title(self, keyword: str) -> list:
        """
        Given the lod containing the volumes extracted through keyword matching, takes the entries corresponding
        to the types of keyword and splits the matched string by the short title into two parts: [title, short].
        The exception is when the keyword is colocated, which only contains the short title.
        Args:
            keyword(str): the matching keyword to look into the dict
        Returns:
            list[dict]: volumes where keyword contains information. Try to match a short title by regex and if found,
                        said information is split into two parts [title, short].
                        title is a list of possible titles by being before
                        the short title and the short is a single string.
                        If no match is found in a string, the sets are left empty.
                        If the keyword is colocated, only set the short title and the loctime info.
        """

        # the colocated attribute alone contains only the short title
        if keyword == "colocated":
            extractList = [{"number": info["number"], keyword: info[keyword], "title": [],
                            "short": info[keyword], "loctime": info["loctime"]}
                           for info in self.remaining_events if info[keyword]]
            return extractList

        extractList = []
        # match words in brackets
        shortRegex = re.compile("\(([^)]*)\)")
        # match containing 2 captial letters but not USA. Do not seperately capture the year: (?:
        shortRegex2 = re.compile("([^\s)]*(?!USA)[A-Z]{2}[^\s)]*\s?(?:[0-9]{4})?)")
        for info in self.remaining_events:
            if info[keyword]:
                befores = []
                shorts = []
                for text in info[keyword]:
                    split = re.split(shortRegex, text, maxsplit=1)
                    if len(split) > 1:
                        befores.append(split[0])
                        shorts.append(split[1])
                        continue

                    else: split = re.split(shortRegex2, text, maxsplit=1)
                    if len(split) == 1: continue  # length 1 denotes no match, hence string was not split

                    befores.append(split[0])
                    shorts.append(split[1])
                if shorts:
                    extract = {"number": info["number"], keyword: info[keyword], "title": befores,
                               "short": shorts[0], "loctime": info["loctime"]}
                else:
                    extract = {"number": info["number"], keyword: info[keyword], "title": befores,
                               "short": "", "loctime": info["loctime"]}
                extractList.append(extract)
        return extractList

    def extract_times(self, texts: List[str]):
        """
        Extract times from a list of texts using nlp
        """
        times = []
        for text in texts:
            doc = self.nlp(text)
            times.extend([entity.text for entity in doc.ents if entity.label_ == "DATE"])
        return times

    def extract_location(self, texts: List[str]):
        """
        Extract locations from a list of texts using nlp
        """
        locs = []
        for text in texts:
            doc = self.nlp(text)
            locs.extend([entity.text for entity in doc.ents if entity.label_ == "GPE"])
        return locs

    def match_month(self, texts: List[str]):
        """
        Extract a single month from a list of texts using regex
        """
        matched = None
        for text in texts:
            matched = re.search(self.month_regex, text)
            if matched: return matched[0]

    def match_year(self, texts: List[str]):
        """
        Extract a single year from a list of texts using regex
        """
        matched = None
        for text in texts:
            matched = re.search(self.year_regex, text)
            if matched: return matched[0]

    def loctime_year(self, text: str):
        """
        Extract the year from the loctime attribute
        """
        if text is None: return None
        matched = re.search(self.year_regex, text)
        if matched: return matched[0]

    def loctime_month(self, text: str):
        """
        Extract the month from the loctime attribute
        """
        if text is None: return None
        matched = re.search(self.month_regex, text)
        if matched: return matched[0]

    def loctime_location(self, text: str):
        """
        Extract the location from the loctime attribute
        """
        if text is None: return []
        try:
            return text.split(", ")[0:2]
        except IndexError:  # catch corner cases, where loctime attribute is not correctly formatted
            return []

    def extract_time_and_place(self, df: pd.DataFrame, keyword: str) -> pd.DataFrame:
        """
        Takes a dataframe based on a lod and extracts time and location information
        based on the loctime attribute and the original texts accessed through keyword
        Args:
            df(pd.DataFrame): DataFrame of the events lod to extract info from
            keyword(str): extraction key, which supplies the additional info on top of loctime
        Returns:
            pd.DataFrame: df with information extracted
        """

        # use regex to get information from loctime
        df["month"] = df["loctime"].map(self.loctime_month)
        df["year"] = df["loctime"].map(self.loctime_year)
        df["locations"] = df["loctime"].map(self.loctime_location)

        # if the keyword is not colocated, try extracting further info
        if keyword != "colocated":
            # use nlp to get information when loctime was not set
            df["dates"] = df[keyword].map(self.extract_times)

            df.loc[pd.isna(df['loctime']), "month"] = df["dates"].map(self.match_month)
            df.loc[pd.isna(df['loctime']), "year"] = df["dates"].map(self.match_year)
            df.loc[pd.isna(df['loctime']), "locations"] = df[keyword].map(self.extract_location)

            df.drop(columns=["dates"])

        df.loc[pd.isna(df['year']), "year"] = df["short"].map(self.loctime_year)
        df["month"] = df["month"].map(lambda x: self.month_numerizer[x.lower()] if x else None)

        df["loc1"] = df["locations"].map(lambda l: l[0] if l else None)
        df["loc2"] = df["locations"].map(lambda l: l[1] if len(l) > 1 else None)

        # use country converter to get ISO3 names
        cc = coco.CountryConverter()
        coco_logger = coco.logging.getLogger()
        coco_logger.setLevel(50)  # supress coco conversion output

        df["countryISO3"] = cc.pandas_convert(series=df["loc2"], to="ISO3", not_found="None")
        df.loc[df["countryISO3"] == "None", "countryISO3"] = (
            cc.pandas_convert(series=df["loc1"], to="ISO3", not_found="None"))

        df = df.astype({"countryISO3": str})

        # remove columns not used for matching
        df = df.drop(columns=["loc1", "loc2", "loctime", "locations", keyword])

        return df

    def get_loctime_info(self, keyword: str) -> pd.DataFrame:
        """
        Given the lod containing the volumes extracted through keyword matching,
        tries to extract guesses for the title, short, time and location.
        Args:
            keyword(str): the matching keyword to look into the dict
        Returns:
            pd.DataFrame: the events with additional extracted information using the keyword extract
        """

        title_split = self.split_by_short_title(keyword)

        if not title_split:  # no remaining volumes given the keyword
            return pd.DataFrame(columns=["number", "title", "loctime", "month", "year", "countryISO3", "short"])
        return self.extract_time_and_place(
            pd.DataFrame.from_dict(title_split), keyword)


class TitleExtractor():
    """
    Class to extract attributes about event(proceedings) from their title.
    """
    def __init__(self):
        # self.processor = ExtractionProcessor(dict())  # need an instance to use the methods
        self.keyword = "title_copy"

    # def extract_short(self, events: pd.DataFrame) -> pd.DataFrame:
    #     """
    #     Helper function.
    #     Given the events with a column 'title', extract the short title.
    #     Args:
    #         events(pandas.DataFrame): DataFrame of interest containing a 'title' column
    #     Returns:
    #         pd.DataFrame: events with additional info 'short'.
    #     """

    def extract_attributes(self, events: pd.DataFrame) -> pd.DataFrame:
        """
        Given the events with a column 'title', extract matching attributes.
        Args:
            events(pandas.DataFrame): DataFrame of interest containing a 'title' column
        Returns:
            pd.DataFrame: events with additional info 'month', 'year', 'countryISO3', 'short'.
        """
        events = events.copy()  # suppress pandas warnings
        number = "number" in events.columns
        events["loctime"] = None
        if not number:
            events["number"] = -1

        events[self.keyword] = events["title"].map(lambda x: [x])  # put title into list
        event_lod = events.to_dict(orient='records')
        processor = ExtractionProcessor(event_lod)

        extract: pd.DataFrame = processor.get_loctime_info(self.keyword)
        # we will take the title from the original events
        extract = extract.drop(columns=["title", "dates"])

        events = events.drop(columns=["loctime", self.keyword])
        if not number:
            extract = extract.drop(columns="number")
            events = events.drop(columns="number")

        extract = events.join(extract)

        return extract
