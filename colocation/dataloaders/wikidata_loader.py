'''
Created on 2023-06-09
@author: nm
'''

from colocation.cache_manager import CsvCacheManager
from lodstorage.query import Query
from lodstorage.sparql import SPARQL
import pandas as pd
from typing import List, Dict

wikidata_cacher = CsvCacheManager(base_folder="wikidata")


def get_workshop_ids_from_lod(lod: List[Dict]) -> List[str]:
    """
    Takes a lod with wikidata_event keys and returns the
    list of wikidata ids
    Args:
        lod(list(dict)): lod to extract workshop ids from
    """

    found = [dici['wikidata_event'] for dici in lod]
    found = [f for f in found if f is not None]
    found = [event for events in found for event in events]
    found = [str(event).split('/')[-1] for event in found]
    found = [s for s in found if s != "None"]
    found = ["wd:" + s for s in found]
    return found


def query_wikidata(query: Dict[str, str]) -> pd.DataFrame:
    """
    Runner for wikidata queries given the query string
    """
    endpoint_url = "https://query.wikidata.org/sparql"
    endpoint = SPARQL(endpoint_url)
    query = Query(**query)

    try:
        lod = endpoint.queryAsListOfDicts(query.query)
        df = pd.DataFrame(lod)

        return df
    except Exception as ex:
        print(f"{query.title} at {endpoint_url} failed: {ex}")
        return Exception(ex)


def get_wikidata_workshops(workshop_ids: List[str], name: str, reload: bool = False) -> pd.DataFrame:
    """
    Use a SPARQL query to get all workshops from the given list of ids from Wikidata.
    Cache the result using the specified name and reuse, unless reload is specified.

    Args:
        workshop_ids(list(str)) : list of the ids for the workshops to query wikidata for
        name(str) : name to differentiate queries for different purposes
        reload(bool) : whether to force reload the conferences instead of taking from cache
    """
    name = f"workshops_{name}"
    df = wikidata_cacher.load_csv(name)
    if not reload and df is not None:
        return df

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
    df = query_wikidata(workshop_query)
    renaming = {"workshopLabel": "title", "locationLabel": "locations"}
    df = format_frame(df, renaming)
    wikidata_cacher.store_csv(name, df)

    return df


def get_wikidata_workshops_by_number(workshop_numbers: List[int], name: str, reload: bool = False) -> pd.DataFrame:
    """
    Use a SPARQL query to get as many workshops from the given list of ids as possible.
    Cache the result using the specified name and reuse, unless reload is specified.

    Args:
        workshop_ids(list(str)) : list of the CEUR-WS series number of the workshops
        name(str) : name to differentiate queries for different purposes
        reload(bool) : whether to force reload the workshops instead of taking from cache
    """
    df = wikidata_cacher.load_csv(name)
    if not reload and df is not None:
        return df

    workshop_numbers = [f'"{num}"' for num in workshop_numbers]

    workshop_query = {
        "lang": "sparql",
        "name": "WS",
        "title": "Workshops",
        "description": "Wikidata SPARQL query getting academic workshops based on series number",
        "query": f"""
select ?number ?Wikidata
where {{
  ?proceedings wdt:P179 wd:Q27230297.
  ?proceedings p:P179 ?ceur.
  ?ceur pq:P478 ?number.
  ?proceedings wdt:P4745 ?Wikidata.
  values ?number {{{' '.join(workshop_numbers)}}}
}}
"""
    }

    df = query_wikidata(workshop_query)
    df = df.astype({"number": int})
    df = df.rename(columns={"number": "Ceur-WS"})
    wikidata_cacher.store_csv(name, df)

    return df


def get_wikidata_conferences(reload: bool = False) -> pd.DataFrame:
    """
    Use a SPARQL query to get all conferences from Wikidata.
    Cache the result and reuse, unless reload is specified.

    Args:
        reload(bool) : whether to force reload the conferences instead of taking from cache
    """
    name = "conferences"
    df = wikidata_cacher.load_csv(name)
    if not reload and df is not None:
        return df

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

    df = query_wikidata(conference_query)
    renaming = {"conferenceLabel": "title"}
    df = format_frame(df, renaming)
    wikidata_cacher.store_csv(name, df)

    return df


def format_frame(df: pd.DataFrame, renaming: Dict[str, str]) -> pd.DataFrame:
    """
    Format dataframe such that it contains the columns required for matching.
    Required columns: short, title, countryISO3, month, year
    Args:
        df(pd.DataFrame): workshop or conference dataframe to format
        renaming(dict[str,str]): columns will be renamed after the given scheme
    """
    df = df.rename(columns=renaming)

    df.loc[pd.isna(df['timepoint']), "timepoint"] = df["start"]
    df.loc[pd.isna(df['timepoint']), "timepoint"] = df["end"]
    df.loc[pd.isna(df['timepoint']), "timepoint"] = None

    df["month"] = df["timepoint"].dt.month
    df["year"] = df["timepoint"].dt.year

    return df


def get_wikidata_dblp_info(conference_ids: List[str], name: str, reload: bool = False) -> pd.DataFrame:
    """
    Use a SPARQL query to potentially get dois and dblp links for conferences from the given list of ids from Wikidata.
    Cache the result using the specified name and reuse, unless reload is specified.

    Args:
        conference_ids(list(str)) : list of the ids for the conferences to query wikidata for
        name(str) : name to differentiate queries for different purposes
        reload(bool) : whether to force reload the conferences instead of taking from cache

    Returns:
        pandas.DataFrame: successfully connected elements with the columns
            'conference', 'proc', 'dblp_event', 'dblp_proceedings', 'uri'
    """

    name = f"dblp_{name}"
    df = wikidata_cacher.load_csv(name)
    if not reload and df is not None:
        return df

    conference_ids = ["wd:" + c for c in conference_ids]

    dblp_query = {
        "lang": "sparql",
        "name": "Wikidata Dblp",
        "title": "Dblp",
        "description": "Wikidata SPARQL query getting connecting information to dblp",
        "query": f"""
SELECT distinct ?conference (sample(?proc) as ?proc) (sample(?dblp_event) as ?dblp_event)
(sample(?dblp_proceedings) as ?dblp_proceedings) (sample(?uri) as ?uri)
WHERE
{{
  VALUES ?conference {{{" ".join(conference_ids)}}}.
  ?conference wdt:P31/wdt:P279* wd:Q2020153.
  optional {{?conference wdt:P10692 ?dblp_event.}}
  optional {{
    ?proc wdt:P4745 ?conference;
          wdt:P8978 ?dblp_proceedings.
  }}
  optional {{
    ?conference wdt:P973 ?uri.
    Filter regex(str(?uri), "dblp", "i").
  }}
}}
Group by ?conference
"""
    }

    df = query_wikidata(dblp_query)
    df = df.reindex(["conference", "proc", "dblp_event", "dblp_proceedings", "uri"], axis=1)
    wikidata_cacher.store_csv(name, df)

    return df
