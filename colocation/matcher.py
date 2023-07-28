'''
Created on 2023-04-21

'''
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from .extractor import matchtypes


class Matcher:
    """
    match different types of events
    """

    def __init__(self, types_to_match: list = None):
        """
        constructor

        Args:
            types_to_match(list): list of matchtypes if only a subset of the possible types should be matched
        """
        self.matchtypes = types_to_match if types_to_match else matchtypes

    @staticmethod
    def match_same_type(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
        """
        Matches events of the same type, so conferences with conferences and
        workshops with workshops requiring df1 and df2 to have the columns
        short, title, countryISO3, month, year.

        Args:
            df1(pandas.DataFrame): dataframe with one side of the events.
            df2(pandas.DataFrame): dataframe with the matching target.
        Returns:
            pandas.DataFrame: DataFrame that holds the pairs that are matched to be the same type
        """
        return NotImplementedError()

    @staticmethod
    def fuzzy_title_matching(workshops: pd.DataFrame,
                             conferences: pd.DataFrame, threshold: float) -> pd.DataFrame:
        """
        Uses td-idf embedding and cosine similarity to match titles.
        Since titles of conferences from different years are extremely similar, also use the year
        to refine which titles are valid matches.

        Args:
            workshops(pandas.DataFrame): workshops as used in match_extract
            conferences(pandas.DataFrame): conferences as used in match_extract
            threshold(float): threshold value when titles should be seen as similar
        Returns:
            pandas.DataFrame: workshops matched with conferences
        """

        corpus = list(workshops["W.title"])
        len_work = len(corpus)

        corpus.extend(list(conferences["C.title"]))
        # vectorize corpus using td-idf
        vectorizer = TfidfVectorizer(max_df=0.7)  # ignore stopwords
        X = vectorizer.fit_transform(corpus)

        # get pairwise cosine similarity
        cos = cosine_similarity(X)

        # remove symmetry and zero all matches within same type
        cos = np.triu(cos, k=1)  # only keep upper triangular matrix with 0 diagonal
        cos[0:len_work, 0:len_work] = 0
        cos[len_work:, len_work:] = 0

        # find matches according to the similarity and threshold value
        matches = np.argwhere(cos >= threshold)

        # make copies since we start changing the dataframes
        w = workshops.copy()
        c = conferences.copy()

        # mark found matches
        w["W.partner"] = np.nan
        c["C.partner"] = np.nan

        # further check year
        for num, match in enumerate(matches):
            workshop = workshops.iloc[match[0]]
            conference = conferences.iloc[match[1] - len_work]

            if (pd.notna(workshop["W.year"]) and pd.notna(conference["C.year"]) and
                    int(workshop["W.year"]) == int(conference["C.year"])):
                w.at[match[0], "W.partner"] = num
                c.at[match[1] - len_work, "C.partner"] = num

        # now remove all unmatched rows
        w = w[pd.notna(w["W.partner"])]
        c = c[pd.notna(c["C.partner"])]

        # finally try to match on identifiers
        match1 = w.merge(c, left_on=["W.partner", "W.countryISO3"], right_on=["C.partner", "C.countryISO3"])
        match2 = w.merge(c, left_on=["W.partner", "W.month"], right_on=["C.partner", "C.month"])

        match1 = match1[match1["W.countryISO3"] != "None"]
        match2 = match2[pd.notna(match2["W.month"])]

        res = pd.concat([match1, match2], ignore_index=True)
        res = res.drop(columns=["W.partner", "C.partner"])
        res = res.drop_duplicates(subset=["W.title", "W.short", "C.title", "C.short"])

        return res

    def match_extract(self, extract_function, remove_function, remove_key: str,
                      conferences: pd.DataFrame, threshold: float) -> pd.DataFrame:
        """
        Matches the extract found from worshops using the iterative matching process
        to the given conferences.
        Requires the attributes short, title, countryISO3, month, year.

        Args:
            extract_function(keyword: str): that provides the extracted info as a DataFrame
            when given the appropriate keyword.
            remove_function(remove_key: str, keys_to_remove: list): function to remove sucessfully matched elements
            remove_key(str): column in the DataFrame produced by extract function that contains the
                             values with which to call the remove_function
            conferences(pandas.DataFrame): Conferences with matchable attributes. Required
            to have the columns short, title, locations, month, year to match against.
            threshold(float): threshold value when titles should be seen as similar by fuzzy matching
        Returns:
            pandas.DataFrame: DataFrame that holds workshops and the conferences that they have matched with
        """

        # rename columns to control join operations
        conf = conferences.rename(columns={old: f"C.{old}" for old in conferences.columns})
        work = extract_function(self.matchtypes[0])
        work = work.rename(columns={old: f"W.{old}" for old in work.columns})

        if type(conf["C.title"].iloc[0]) == list:
            conf["C.title"] = conf["C.title"].map(lambda l: l[0] if l else "")

        res = pd.DataFrame(columns=list(work.columns).extend(list(conf.columns)))

        iterative_match_list = self.matchtypes
        iterative_match_list.insert(1, "colocated")
        for match_type in iterative_match_list:

            # first work DataFrame is initialized before the loop
            if match_type != matchtypes[0]:
                work = extract_function(match_type)
                work = work.rename(columns={old: f"W.{old}" for old in work.columns})

            # decide how to handle multiple titles
            # for now just take the first one. TODO potential improvement
            if type(work["W.title"].iloc[0]) == list:
                work["W.title"] = work["W.title"].map(lambda l: l[0] if l else "")

            # first matching condition is matching short titles and country
            match1 = work.merge(conf, left_on=["W.short", "W.countryISO3"],
                                right_on=["C.short", "C.countryISO3"])

            # second matching condition is matching short titles and month
            work = work.astype({"W.month": "str"})
            conf = conf.astype({"C.month": "str"})
            match2 = work.merge(conf, left_on=["W.short", "W.month"],
                                right_on=["C.short", "C.month"])

            match1 = match1[match1["W.countryISO3"] != "None"]
            match2 = match2[pd.notna(match2["W.month"])]

            # remaining matching conditions are matching titles and year with additional identifier
            match3 = self.fuzzy_title_matching(work, conf, threshold=threshold)

            # add found matches to result
            new = pd.concat([match1, match2, match3], ignore_index=True)
            res = pd.concat([res, new], ignore_index=True)

            # remove double matches
            # perhaps cleaner when given an id column for the conferences
            res = res.drop_duplicates(subset=[f"W.{remove_key}", "C.title", "C.short"])

            # remove matched workshops from continuing iterations
            to_remove = list(new[f"W.{remove_key}"])
            remove_function(remove_key, to_remove)

        return res
