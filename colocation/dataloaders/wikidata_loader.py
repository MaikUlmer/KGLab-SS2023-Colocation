'''
Created on 2023-06-09
@author: nm
'''

from lodstorage.query import Query
from lodstorage.sparql import SPARQL
import os
import pandas as pd
from pathlib import Path


def get_workshop_ids_from_lod(lod: list) -> list:
    """
    Takes a lod with wikidata_event keys and returns the
    list of wikidata ids
    Args:
        lod(list(dict)): lod to extract workshop ids from
    """

    # TODO handle wikidata not returning any result for certain ids

    found = [dici['wikidata_event'] for dici in lod]
    found = [f for f in found if f is not None]
    found = [event for events in found for event in events]
    found = [str(event).split('/')[-1] for event in found]
    found = [s for s in found if s != "None"]
    found = ["wd:" + s for s in found]
    return found


def get_wikidata_workshops(workshop_ids: list, name: str, reload: bool = False) -> pd.DataFrame:
    """
    Use a SPARQL query to get all workshops from the given list of ids from Wikidata.
    Cache the result using the specified name and reuse, unless reload is specified.

    Args:
        workshop_ids(list(str)) : list of the ids for the workshops to query wikidata for
        name(str) : name to differentiate queries for different purposes
        reload(bool) : whether to force reload the conferences instead of taking from cache
    """
    root_path = f"{Path.home()}/.ceurws"
    os.makedirs(root_path, exist_ok=True)
    store_path = root_path + f"/wikidata_workshops_{name}.csv"

    if os.path.isfile(store_path) and not reload:
        return pd.read_csv(store_path)

    # TODO handle wikidata line limit for values clause

    workshop_query = {
        "lang": "sparql",
        "name": "WS",
        "title": "Workshops",
        "description": "Wikidata SPARQL query getting academic workshops with relevant information",
        "query": f"""
SELECT distinct ?workshop ?workshopLabel ?short ?countryISO3 ?locationLabel ?start ?end ?timepoint
WHERE
{{
  VALUES ?workshop {{{" ".join(workshop_ids)}}}.
  ?workshop wdt:P31/wdt:P279* wd:Q40444998;
            rdfs:label ?workshopLabel.
  FILTER langMatches(lang(?workshopLabel), "en")
  OPTIONAL {{ ?workshop wdt:P1813 ?short.}}
  optional {{ ?workshop wdt:P17 ?country.
              ?country wdt:P298 ?countryISO3.}}
  optional {{ ?workshop wdt:P276 ?location;
                        rdfs:label ?locationLabel.
              FILTER langMatches(lang(?locationLabel), "en") }}
  optional {{ ?workshop wdt:P580 ?start.}}
  optional {{ ?workshop wdt:P582 ?end.}}
  optional {{ ?workshop wdt:P585 ?timepoint.}}
}}
"""
    }
    endpoint_url = "https://query.wikidata.org/sparql"
    endpoint = SPARQL(endpoint_url)
    query = Query(**workshop_query)

    try:
        lod = endpoint.queryAsListOfDicts(query.query)
        df = pd.DataFrame(lod)
        renaming = {"workshopLabel": "title", "locationLabel": "locations"}
        df = format_frame(df, renaming)
        df.to_csv(store_path, index=False)

        return df
    except Exception as ex:
        print(f"{query.title} at {endpoint_url} failed: {ex}")
        return Exception(ex)


def get_wikidata_conferences(reload: bool = False) -> pd.DataFrame:
    """
    Use a SPARQL query to get all conferences from Wikidata.
    Cache the result and reuse, unless reload is specified.

    Args:
        reload(bool) : whether to force reload the conferences instead of taking from cache
    """
    root_path = f"{Path.home()}/.ceurws"
    os.makedirs(root_path, exist_ok=True)
    store_path = root_path + "/wikidata_conferences.csv"

    if os.path.isfile(store_path) and not reload:
        return pd.read_csv(store_path)

    conference_query = {
        "lang": "sparql",
        "name": "Cf",
        "title": "Conferences",
        "description": "Wikidata SPARQL query getting academic conferences with relevant information",
        "query": """
SELECT distinct ?conference ?conferenceLabel ?short ?countryISO3 ?start ?end ?timepoint
WHERE
{
  ?conference wdt:P31/wdt:P279* wd:Q2020153;
              rdfs:label ?conferenceLabel.
  FILTER langMatches(lang(?conferenceLabel), "en")
  SERVICE wikibase:label {bd:serviceParam wikibase:language "en". }
  OPTIONAL { ?conference wdt:P1813 ?short.}
  optional { ?conference wdt:P17 ?country.
           ?country wdt:P298 ?countryISO3.}
  optional { ?conference wdt:P580 ?start.}
  optional { ?conference wdt:P582 ?end.}
  optional { ?conference wdt:P585 ?timepoint.}
}
"""
    }
    endpoint_url = "https://query.wikidata.org/sparql"
    endpoint = SPARQL(endpoint_url)
    query = Query(**conference_query)

    try:
        lod = endpoint.queryAsListOfDicts(query.query)
    except Exception as ex:
        print(f"{query.title} at {endpoint_url} failed: {ex.with_traceback()}")
        raise ex

    df = pd.DataFrame(lod)
    renaming = {"conferenceLabel": "title"}
    df = format_frame(df, renaming)
    df.to_csv(store_path, index=False)

    return df


def format_frame(df: pd.DataFrame, renaming: dict) -> pd.DataFrame:
    """
    Format dataframe such that it contains the columns required for matching.
    Required columns: short, title, countryISO3, month, year
    Args:
        df(pd.DataFrame): workshop or conference dataframe to format
        renaming(dict[str,str]): columns will be renamed after the given scheme
    """
    df = df.rename(columns=renaming)

    def time_extraction(text: str, index: int):
        """
        Extract the year or month from the loctime attribute
        """
        if not text: return None
        else: return text.split('-')[index]

    df.loc[pd.isna(df['timepoint']), "timepoint"] = df["start"]
    df.loc[pd.isna(df['timepoint']), "timepoint"] = df["end"]
    df.loc[pd.isna(df['timepoint']), "timepoint"] = None

    df["month"] = df["timepoint"].dt.month
    df["year"] = df["timepoint"].dt.year

    return df