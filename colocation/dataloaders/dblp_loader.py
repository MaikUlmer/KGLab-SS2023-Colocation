'''
Created on 2023-07-28
@author: nm
'''

from lodstorage.query import Query
from lodstorage.sparql import SPARQL
import os
import pandas as pd
from pathlib import Path
from typing import List, Dict
import re


def query_dblp(query) -> List[Dict]:
    """
    Helper function that performs the Dblp query.

    Args:
        query: pylodstorage query to execute.

    Returns:
        list(dict): resulting lod
    """
    endpoint_url = "https://sparql.dblp.org/sparql"
    endpoint = SPARQL(endpoint_url)
    q = Query(**query)

    try:
        lod = endpoint.queryAsListOfDicts(q.query)
    except Exception as ex:
        print(f"{q.title} at {endpoint_url} failed: {str(ex)}")
        raise ex

    return lod


def guess_dblp_conference(workshop_df: pd.DataFrame) -> pd.DataFrame:
    """
    Helper function for get_dblp_workshops.
    Takes the workshop id and guesses the id of the proceedings of the co-located conference as described in the readme.

    Args:
        workshop_df(pandas.DataFrame): DataFrame containing the sparql response for Ceur-WS volumes.

    Returns:
        pandas.DataFrame: Same DataFrame enriched by a column 'conference_guess'.
    """

    # write regex to match the string after the year number right before the end
    regex = re.compile("(?<=[0-9])[a-zA-Z].*$")  # delete everthing beginning with the first letter after the year

    workshop_df["conference_guess"] = workshop_df["volume"].map(lambda x: re.sub(regex, '', x))

    return workshop_df


def verify_dblp_uris(dblp_uris: pd.Series) -> pd.Series:
    """
    Takes a column of Dblp uris and uses a sparql query to verify whether any given one exists in Dblp.

    Args:
        dblp_uris(pandas.Series): column of uris to check.

    Returns:
        pandas.Series: column of the same shape as the input with corresponding truth value.
    """
    uris = [f"<{proceeding}>" for proceeding in dblp_uris.to_list()]

    verify_query = {
        "lang": "sparql",
        "name": "Verify uris",
        "title": "Verification",
        "description": "Dblp SPARQL query checking if the uri has any property",
        "query": f"""
PREFIX dblp: <https://dblp.org/rdf/schema#>
select distinct ?dblp
where {{
  Values ?dblp {{ {" ".join(uris)} }}.
  ?dblp ?p ?o.
}}
"""
    }

    lod = query_dblp(verify_query)
    results = [res["dblp"] for res in lod]

    truth_column = dblp_uris.map(lambda x: x in results)
    return truth_column


def verify_dblp_events(dblp_uris: pd.Series, dblp_event_urls: pd.Series) -> pd.Series:
    """
    Takes a column of Dblp uris and their supposed corresponding event and
    uses a sparql query to verify whether any given one exists in Dblp.

    Args:
        dblp_uris(pandas.Series): column of uris to base the check.
        dblp_event_urls(pd.Series): column of urls to check.

    Returns:
        pandas.Series: column of the same shape as the input with corresponding truth value.
    """
    if (a := dblp_uris.shape[0]) != (b := dblp_event_urls.shape[0]):
        raise ValueError(f"Series have different shapes: {a,b}.")

    uris = [f"<{proceeding}>" for proceeding in dblp_uris.to_list()]

    verify_query = {
        "lang": "sparql",
        "name": "Verify event url",
        "title": "Verification",
        "description": "Dblp SPARQL query getting the toc page of the conference proceeedings",
        "query": f"""
PREFIX dblp: <https://dblp.org/rdf/schema#>
select distinct ?dblp ?event
where {{
  Values ?dblp {{ {" ".join(uris)} }}.
  ?dblp dblp:listedOnTocPage ?event.
}}
"""
    }

    lod = query_dblp(verify_query)
    assingment_dict = {pair["dblp"]: pair["event"] for pair in lod}

    toc_pages = dblp_uris.map(lambda x: assingment_dict[x] if x in assingment_dict else None)

    truth_column = dblp_event_urls == toc_pages
    return truth_column


def get_dblp_workshops(workshop_numbers: List[int], name: str, number_key: str = "number",
                       reload: bool = False) -> pd.DataFrame:
    """
    Use a SPARQL query to get all Ceur-WS workshops from the given list of ids from Dblp.
    Cache the result using the specified name and reuse, unless reload is specified.

    Args:
        workshop_numbers(list(int)) : list of the numbers for the ceur-ws workshops.
        name(str) : name to differentiate queries for different purposes.
        number_key(str): name the number attribute should have
        reload(bool) : whether to force reload the conferences instead of taking from cache.

    Returns:
        pandas.DataFrame: DataFrame containing relevant information about the Ceur-WS volumes,
                          including guess for proceedings of the co-located conference.
    """
    root_path = f"{Path.home()}/.ceurws"
    os.makedirs(root_path, exist_ok=True)
    store_path = root_path + f"/dblp_workshops_{name}.csv"

    if os.path.isfile(store_path) and not reload:
        return pd.read_csv(store_path)

    workshop_query = {
        "lang": "sparql",
        "name": "DWS",
        "title": "DWorkshops",
        "description": "Dblp SPARQL query getting academic workshops with relevant information",
        "query": f"""
PREFIX datacite: <http://purl.org/spar/datacite/>
PREFIX dblp: <https://dblp.org/rdf/schema#>
PREFIX litre: <http://purl.org/spar/literal/>
SELECT ?{number_key} ?volume ?dblpid ?urn
WHERE{{
?volume dblp:publishedIn "CEUR Workshop Proceedings" ;
    dblp:publishedInSeries "CEUR Workshop Proceedings" ;
    dblp:publishedInSeriesVolume ?{number_key}.
    VALUES ?{number_key} {{{" ".join([f'"{number}"' for number in workshop_numbers])}}}.  # dblp needs number as string
    ?volume datacite:hasIdentifier ?s.
    ?s	datacite:usesIdentifierScheme datacite:dblp-record ;
        litre:hasLiteralValue ?dblpid ;
        a datacite:ResourceIdentifier.
    optional {{
    ?volume datacite:hasIdentifier ?s2.
    ?s2	datacite:usesIdentifierScheme datacite:urn ;
        litre:hasLiteralValue ?urn ;
        a datacite:ResourceIdentifier.
    }}
 }}
"""
    }

    lod = query_dblp(workshop_query)
    df = pd.DataFrame(lod)

    df = guess_dblp_conference(df)

    df.to_csv(store_path, index=False)

    return df


def get_dblp_conferences(reload: bool = False) -> pd.DataFrame:
    """
    Use a SPARQL query to get all conferences from Dblp.
    Cache the result and reuse, unless reload is specified.

    Args:
        reload(bool) : whether to force reload the conferences instead of taking from cache.

    Returns:
        pandas.DataFrame: conferences with columns 'volume', 'event', 'title', 'doi'
    """
    root_path = f"{Path.home()}/.ceurws"
    os.makedirs(root_path, exist_ok=True)
    store_path = root_path + "/dblp_conferences.csv"

    if os.path.isfile(store_path) and not reload:
        return pd.read_csv(store_path)

    conference_query = {
        "lang": "sparql",
        "name": "DCf",
        "title": "DConferences",
        "description": "Dblp SPARQL query getting academic conferences with relevant information",
        "query": """
PREFIX datacite: <http://purl.org/spar/datacite/>
PREFIX litre: <http://purl.org/spar/literal/>
PREFIX dblp: <https://dblp.org/rdf/schema#>
select distinct ?volume ?event ?title ?doi
where {
  ?volume dblp:title ?title.
  FILTER regex(?title, "conference", "i")
  FILTER regex(str(?volume), "conf", "i")
  ?volume datacite:hasIdentifier ?s;
          dblp:listedOnTocPage ?event.
    ?s	datacite:usesIdentifierScheme datacite:dblp-record ;
        litre:hasLiteralValue ?dblpid ;
        a datacite:ResourceIdentifier.
  optional {
    ?volume datacite:hasIdentifier ?s2.
    ?s2	datacite:usesIdentifierScheme datacite:doi ;
        litre:hasLiteralValue ?doi;
        a datacite:ResourceIdentifier.
  }
}
"""
    }
    lod = query_dblp(conference_query)
    df = pd.DataFrame(lod)

    # drop workshops
    df = df[df["title"].map(lambda x: "workshop" not in x.lower())]

    # add virtual collective proceedings for split prceedings
    split = df[df["volume"].str.contains("-[0-9]+$", na=False, regex=True)]
    split = split.copy()
    split["volume"] = split["volume"].map(lambda x: x[0:-2])
    split.drop_duplicates(subset="volume")

    df = pd.concat([df, split])

    df.to_csv(store_path, index=False)

    return df


def dblp_events_to_proceedings(events: pd.Series) -> pd.Series:
    """
    Given the links to dblp events like
    'https://dblp.org/db/conf/pqcrypto/pqcrypto2014' returns the links to the proceedings like
    'https://dblp.org/rec/conf/pqcrypto/2014' using a guess, since we cannot for sure differentiate
    workshops co-located with the conference from the conference using sparql.

    Args:
        events(pandas.Series): event ids to get proceedings ids from.

    Returns:
        pandas.Series: event ids replaced with proceedings ids.
    """
    regex = re.compile("[a-zA-Z]*(?=[0-9]{4}$)")
    db = re.compile("db/")
    events = events.map(lambda event: re.sub(db, "rec/", event) if event else event, na_action='ignore')
    events = events.map(lambda event: re.sub(regex, "", event) if event else event, na_action='ignore')

    return events


def dblp_proceedings_to_events(proceedings: pd.Series) -> pd.Series:
    """
    Given the links to dblp proceedings like
    'https://dblp.org/rec/conf/pqcrypto/2014' returns the links to the events like
    'https://dblp.org/db/conf/pqcrypto/pqcrypto2014' using a sparql query.

    Args:
        proceedings(pandas.Series): proceedings ids to get event ids from.

    Returns:
        pandas.Series: proceedings ids replaced with event ids.
    """

    pro = [f"<{proceeding}>" for proceeding in proceedings.to_list()]

    transform_query = {
        "lang": "sparql",
        "name": "Proceedings to Event",
        "title": "Proceedings to Event",
        "description": "Dblp SPARQL query getting the Event id for the given proceedings id",
        "query": f"""
PREFIX dblp: <https://dblp.org/rdf/schema#>
select ?dblp ?toc
where {{
  Values ?dblp {{ {" ".join(pro)} }}
  optional {{?dblp dblp:listedOnTocPage ?toc. }}
}}
"""
    }
    lod = query_dblp(transform_query)

    res = pd.DataFrame(lod)["toc"] if lod else pd.Series(data=[])
    return res
