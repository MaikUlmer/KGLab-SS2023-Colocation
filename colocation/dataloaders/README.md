## Dblp downloading
### Crossover matching
For using the established matching method, we need to get the relevant data of all Dblp conferences.
How to do so, without crawling over the index of Dblp as this would involve putting out a number of queries in the order of hundreds of thousands, is not currently known to us.

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

### Wikidata linking

Again we are looking for an alternative to matching properties to identify wikidata conferences with Dblp conferences.  
For this we may again use links provided by our sources.
The *proceedings* of conferences in wikidata *may* have two attributes that allow us to guarantee a match with a certain Dblp conference:

1. The "DBLP publication ID".  
If it is present, it contains a link to the conference id used in Dblp, which in turn is simply part of the html link.
Hence if we have it, we can directly get the corresponding Dblp item.

1. The "DOI".
The digital object identifier is, as the name suggest, an identifier for scientific objects.
Notably, Dblp also holds the DOI of its conference proceedings, so if we extract it during the workshop linking phase, we can link conferences by checking their DOIs for equality.