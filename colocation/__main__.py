'''
Created on 2023-08-16
@author: nm

Main function of the colocation project.
'''
from colocation.dataloaders.wikidata_loader import get_wikidata_conferences
from colocation.cache_manager import JsonCacheManager
from colocation.extractor import ColocationExtractor, ExtractionProcessor
from colocation.matcher import Matcher
from colocation.neo4j_manager import Neo4jManager
from colocation.values import Constants
from colocation.result_processor import ResultProcessor
import pandas as pd
import argparse

MATCH_THREASHOLD = Constants.MATCH_THREASHOLD
LINK_THREASHOLD = Constants.LINK_THREASHOLD

if __name__ == "__main__":

    ################################
    # setup command line interface #
    ################################

    parser = argparse.ArgumentParser(
        prog="Ceur-WS Colocation",
        description="Extracts information from Ceur-WS volumes to link workshops to\
their co-located conference using Wikidata and Dblp as additional datasources."
    )
    parser.add_argument('-r', '--reload', action='store_true', help="Force reload cached results.")
    parser.add_argument('-w', '--write', action='store_true', help="Actually write the updated parameters to Wikidata.")

    args = parser.parse_args()
    reload = args.reload
    write = args.write

    ###########################
    # get Ceur-WS information #
    ###########################
    print("Getting Ceur-WS volumes.")

    cacher = JsonCacheManager()
    volumes = cacher.reload_lod("volumes") if reload else cacher.load_lod("volumes")
    if reload:
        cacher.reload_lod("proceedings")
    extractor = ColocationExtractor(volumes)
    colocation_lod = extractor.get_colocation_info()

    colocation_processor = ExtractionProcessor(colocation_lod)

    #####################################
    # get information from data sources #
    #####################################

    print("Getting extra data from sources.")

    wikidata_conferences = get_wikidata_conferences(reload=reload)

    ##################################
    # match and link events together #
    ##################################

    matcher = Matcher()

    # match Ceur-Ws against wikidata
    print("Matching Ceur-WS volumes against Wikidata conferences.")

    match_workshop_wikidata: pd.DataFrame = matcher.match_extract(
        extract_function=colocation_processor.get_loctime_info,
        remove_function=colocation_processor.remove_events_by_keys,
        remove_key="number",
        conferences=wikidata_conferences,
        threshold=MATCH_THREASHOLD,
        reload=reload,
        save_name="Ceur_Wikidata"
    )

    # get present wikidata conferences found by the matching process
    retained = ["C.conference", "C.title", "C.countryISO3", "C.short", "C.month", "C.year"]
    matched_wikidata_conferences = (
        match_workshop_wikidata[retained]
        .drop_duplicates(subset="C.conference")
        .rename(columns={old: old[2:] for old in retained})
    )
    matched_wikidata_conference_ids = [c.split("/")[-1] for c in matched_wikidata_conferences["conference"]]

    # try to link wikidata conferences to dblp conference proceedings

    links_wikidata_dblp = matcher.link_wikidata_dblp_conferences(
        conference_ids=matched_wikidata_conference_ids,
        name="wikidata_dblp_links",
        reload=reload
    )

    # try to link Ceur-Ws to dblp conference proceedings
    print("Linking Ceur-Ws to Dblp conferences.")

    links_workshop_dblp = matcher.link_workshops_dblp_conferences(
        colocation_lod, reload=reload
    )

    # link split dblp confernece proceedings to virtual node for the entire proceeding
    print("Linking Dblp conferences and virtual nodes.")

    dblp_virtual_links = matcher.link_dblp_split_proceedings(
        potential_virtual_nodes=links_workshop_dblp["C.conference_guess"]
    )

    # supplement the present dblp conferences with attributes for matching
    print("Matching Wikidata and Dblp conferences.")

    linked_dblp_conferences = links_workshop_dblp[
        [c for c in links_workshop_dblp.columns if c[0:2] == "C."]
    ]
    linked_dblp_conferences = linked_dblp_conferences.rename(
        columns={old: old[2:] for old in linked_dblp_conferences.columns}
    )

    # match dblp against wikidata
    match_dblp_wikidata = matcher.match_dataframes_with_title_extract(
        linked_dblp_conferences,
        matched_wikidata_conferences,
        threshold=MATCH_THREASHOLD,
        reload=reload,
        save_name="Wikidata_Dblp",
        to_extract=[1]
    )

    ############################
    # input results into Neo4j #
    ############################

    print("Importing results into Neo4j.")

    neo = Neo4jManager()

    neo.add_matched_nodes(
        match_workshop_wikidata, "number", "conference", "Ceur-WS", "Wikidata"
    )
    neo.add_matched_nodes_undirected(
        links_wikidata_dblp, "conference", "dblp_id", "Wikidata", "Dblp",
        "conference", "conference", "linked"
    )
    neo.add_matched_nodes(
        links_workshop_dblp, "number", "conference_guess",
        "Ceur-WS", "Dblp", relation_type="linked"
    )
    neo.add_matched_nodes(
        dblp_virtual_links, "conference", "virtual", "Dblp", "Dblp",
        "conference", "conference", "linked"
    )
    neo.add_matched_nodes_undirected(
        match_dblp_wikidata,
        key_w="conference_guess", key_c="conference",
        source_w="Dblp", source_c="Wikidata",
        type_w="conference", type_c="conference"
    )

    ######################
    # editing graph data #
    ######################

    print("Editing Neo4j data.")

    neo.set_dblp_virtual()
    neo.create_link_by_workshop_connectivity(
        type_workshop="Ceur-WS",
        type_matched="Wikidata",
        type_linked="Dblp",
        threshold=LINK_THREASHOLD
    )
    neo.delete_match_when_linked("Wikidata", "Dblp")
    neo.add_ceur_attributes(volumes, colocation_lod)
    neo.add_missing_wikidata_event(reload)

    ######################
    # processing results #
    ######################

    print()
    print("Serializing results.")

    neo.serialize_results()

    ###############################
    # writing results to Wikidata #
    ###############################

    processor = ResultProcessor('https://www.wikidata.org/', write=write)
    file_name = "fully_connected_event_present"
    if write:
        res = processor.write_result_to_wikidata(file_name)
        print(f"Wrote co-located attribute for {len(res)} workshops.")
    else:
        res = processor.get_event_conference_pairs(file_name)
        print(f"Would have written co-located attributw for {len(res)} workshops.")
