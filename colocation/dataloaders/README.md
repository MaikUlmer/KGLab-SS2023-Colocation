## Dblp downloading

### Dblp Sparql

We may use the Sparql endpoint Dblp provides to dratically reduce the amount of scraping the Dblp webpages we have to do or even completely eliminate it, since a lot of the information we require is available as RDF information.

Firstly we may find the Ceur-WS volume information by exploiting, that they are published in the Ceur Workshop Proceedings series.
For a given workshop, some interesting parts of the available information is shown below:

```
<https://dblp.org/rec/conf/aaai/2019affcon>
	owl:sameAs <urn:nbn:de:0074-2328-8> ;
	datacite:hasIdentifier [
		datacite:usesIdentifierScheme datacite:dblp-record ;
		litre:hasLiteralValue "conf/aaai/2019affcon" ;
		a datacite:ResourceIdentifier
	], [
		datacite:usesIdentifierScheme datacite:urn ;
		litre:hasLiteralValue "urn:nbn:de:0074-2328-8" ;
		a datacite:ResourceIdentifier
	] ;
	dblp:title "Proceedings of the 2nd Workshop on Affective Content Analysis (AffCon 2019) co-located with Thirty-Third AAAI Conference on Artificial Intelligence (AAAI 2019), Honolulu, USA, January 27, 2019." ;
	dblp:primaryDocumentPage <https://ceur-ws.org/Vol-2328> ;
	dblp:listedOnTocPage <https://dblp.org/db/conf/aaai/affcon2019> ;
	dblp:publishedIn "CEUR Workshop Proceedings" ;
	dblp:publishedInSeries "CEUR Workshop Proceedings" ;
	dblp:publishedInSeriesVolume "2328" ;
	dblp:yearOfPublication "2019" ;
	a dblp:Publication, dblp:Editorship .
```
Notice a few interesting patterns:

1. The Dblp id "https://dblp.org/rec/conf/aaai/2019affcon" contains the conference the workshop is co-located with, namely "https://dblp.org/rec/conf/aaai/2019".
We simply need to drop the workshop acronym after the year.
1. We may get access to different identifiers like the doi, urn or similar.
1. We have access to the volume number, so we can easily link the results to the given workshop. 

For an event in Dblp there are two separate items associated with it:
1. The event: "https://dblp.org/db/conf/aaai/aaai2021.html"
1. The proceedings: "https://dblp.org/rec/conf/aaai/2021.html"

This makes no difference for querying Dblp using Sparql, since both entities correspond to a single collection of RDF triples.
This should however be kept in mind when examining *links into Dblp*, as those may either point towards the event or its proceedings.

### Crossover linking

We may link the Ceur-WS workshops to some Dblp conference by guessing the conference uri as described previously.
Here we only need to check, whether there are conferences with the corresponding uri.

It should be checked, how split proceedings can affect this step.

### Crossover matching
For using the established matching method, we need to get the relevant data of all Dblp conferences.
We can do so by using the previously discussed Sparql endpoint by using a modification of the following query:

```
PREFIX datacite: <http://purl.org/spar/datacite/>
PREFIX litre: <http://purl.org/spar/literal/>
PREFIX dblp: <https://dblp.org/rdf/schema#>
select ?volume ?title 
where {
  ?volume dblp:title ?title.
  FILTER regex(?title, "^proceeding", "i")
  ?volume datacite:hasIdentifier ?s.
	?s	datacite:usesIdentifierScheme datacite:dblp-record ;
		litre:hasLiteralValue ?dblpid ;
		a datacite:ResourceIdentifier. 
}
```
Here we limit the results to proceedings using the regex filter.
Expanding the query by additional attributes and perhaps adding some text processing can yield the attributes needed to perform matching with the workshops.

### Wikidata linking

Again we are looking for an alternative to matching properties to identify wikidata conferences with Dblp conferences.  
For this we may use links provided by our sources.
Depending of if we choose the *conference* itself or its *proceedings*, we may use different properties to reach the corresponding item in Dblp.

1. The "DBLP publication ID".  
This id corresponds to the **proceedings** of a conference in Dblp and may be held by the *proceedings* of a wikidata conference.


1. The "DBLP event ID".  
This id corresponds to the conference **event** itself and is held by the wikidata event.

1. The "described at URL".  
This url gives a link to some site holding information about the conference __event__. Using a regex filter we can limit results to those containing a Dblp link.

Since the uris of these two are different, we need to handle them differently from each other, but we can utilise either one of them, to reach the desired Dblp conference.

As a side effect, we may note when we can only reach a given conference using one path to potentially supply the missing attribute for the other path.

### Query Examples

#### Wikidata Dblp info query
![Alt text](/images/WikidataDblp.png)

#### Dblp Conference query
![Alt text](/images/DblpQuery.png)  
Note: the volume and event are complete Dblp uris and not just what is displayed.

## Problems
The described process runs into a few problems due to the limited degree that Dblp is semantified at the current time.

- Getting from workshops to the co-located conferences is not FAIR. We are instead using patterns in the URIs and hope that these are _universal_ and _unchanging_ over time. Otherwise, there are some workshops that cannot be linked to conferences using our method and there may be a point in time, when the pattern changes, which would break the algorithm for future releases.
- There is no attribute that identifies a conference or a workshop. You can differentiate these from papers and other entities by using a regex filter matching "proceedings", but you cannot differentiate between workshops and conferences without using URI patterns. This results in the problem, that it is FAIR to access an event from its proceedings, but it is __not__ FAIR to do so the other way around. 

# Deprecated
The following methodologies have been superseded by different approaches listed above.

### Link chasing
Instead of getting _all_ conferences, we can also use the CEUR-WS workshops we are trying to match to selectively request the colocated conferences the following way:

1. Use the CEUR-WS series in Dblp to get the html site of the given CEUR-WS workshop.
Note that any htlm document we get from Dblp we will cache to reduce the number of queries we have to make.

1. In the html document, Dblp displays which conference (series) the workshop is a part of:  
![](/images/DblpLinking.png)  
Use a parser like Beatiful Soup to get the relevant links.

1. Get the html pages for the relevant links. Here the link to **IWSECO** would have to be discarded, because it refers to the workshop series and not the co-location conference.

1. The conference series is divided along multiple different instances, each should at the very least contain and link to the proceedings of the given conference:
![](/images/DblpConferenceSeries.png)

1. Get the page for conference proceedings associated to the workshop. For now, we assume that the year should be precise enough to find the correct one.
Since we match through the linking structure of Dblp, we will give these matches a different identifier to the one used in crossover matching.

1. The conference series may also be richer in information than the previous one and also list some of the co-located workshops:
![](/images/DblpInformationRich.png)  
Hence we can search the index file for any CEUR-WS proceedings and save this information for the given workshops.
When we then need to get the co-location conference for a workshop, we can start by checking, if we have previously found the workshop in the list of a conference and if we have filling out this information using this previous hit.  
Note that this may cause precision loss in the case that a workshop is co-located with two conferences like the case with **ISWC 2007 + ASWC 2007** and that the workshop is listed by the conference series of the one and not the other conference.  
Whether this actually occurs should be checked by manual inspection of the Neo4j graph for some known examples like the previous one.