'''
Created on 2023-04-21

'''
import pandas as pd


class Matcher:
    """
    match different types of events
    """

    def __init__(self):
        """
        constructor
        """

    def match_same_type(df1: pd.DataFrame, df2: pd.DataFrame):
        """
        Matches events of the same type, so conferences with conferences and
        workshops with workshops requiring df1 and df2 to have the columns
        short, title, locations, month, year.

        Args:
            df1(pandas.DataFrame): dataframe with one side of the events.
            df2(pandas.DataFrame): dataframe with the matching target.
        Returns:
            TODO
        """
        return NotImplementedError()

    def match_extract(extract_generator, conferences: pd.DataFrame):
        """
        Matches the extract found from worshops using the iterative matching process
        to the given conferences.

        Args:
            extract_generator: instance of a class like ExtractionProcessor with a method
            get_loctime_info(keyword: str) that provides the extracted info as a DataFrame
            when given the appropriate keyword.
            conferences(pandas.DataFrame): Conferences with matchable attributes. Required
            to have the columns short, title, locations, month, year to match against.
            Returns:
            TODO
        """
        return NotImplementedError()
