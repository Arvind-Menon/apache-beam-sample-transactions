# -*- coding: utf-8 -*-
"""
Created on Sun Jul 31 23:13:53 2022

@author: Arvind Menon
"""

import apache_beam as beam
from datetime import datetime


def parse_lines(element):
    """"
    function to split the csv element by comma (',')
    """
    return element.split(',')

# DoFn to convert date column from str to datetime
class convert_timestamp(beam.DoFn):
    def __init__(self, date_format = '%Y-%m-%d', date_index = 0):
        self.date_format = date_format
        self.date_index = date_index
        
    def process(self,element):
        """
        Function to convert date column from str to datetime
        """
        element[self.date_index] = datetime.strptime(element[self.date_index], self.date_format).date()
        yield element

# DoFn to filter dates starting from a given date
class check_date(beam.DoFn):
    def __init__(self, date_format = '%Y-%m-%d', date = '2010-01-01', date_index = 0 ):
        """
        Parameters:
            date_format (str) -- format to convert the date
            date (str) -- the starting date
            date_index -- the index which has the dates in the elements
        """
        self.date_format = date_format
        self.date = date
        self.date_index = 0
        
    def process(self, element):
        """ 
        Function to filter dates starting from a given date
        """
        date_check = datetime.strptime(self.date, self.date_format).date()
        if element[self.date_index] >= date_check:
            yield element
            
# DoFn to check transactions above a given value
class check_value(beam.DoFn):
    def __init__(self, value = 20.0, date_index = 0, value_index = -1):
        """
        Parameters:
            value (float) -- Minimum transaction value to filter
            date_index -- the index which has the dates in the elements
            value_index -- the index which has the transaction_amount in the elements
        """
        self.value = value
        self.date_index = date_index
        self.value_index = value_index
        
    def process(self, element):
        """
        Function to check transactions above a given value
        """
        element[self.value_index] = float(element[self.value_index])
        if element[self.value_index] > self.value:
            yield (str(element[self.date_index]), element[self.value_index])

# DoFn to check transactions above a given value
class convert_to_dict(beam.DoFn):
    def __init__(self, date_index = 0, value_index = -1):
        """
        Parameters:
            date_index -- the index which has the dates in the elements
            value_index -- the index which has the transaction_amount in the elements
        """
        self.date_index = date_index
        self.value_index = value_index

    def process(self, element):
        
        """
        to check transactions above a given value
        """
        yield {'date': element[self.date_index], 'total_amount': element[self.value_index]}

# composite transform 
class MyTransform(beam.PTransform):
  
  def expand(self, input_coll):
    """
    A composite function containing transformations from parsing csv to grouping the dates with total_transactions
    Return:
        a -- pcollection after the transformations
    """
    a = (
        input_coll
        | 'ParseCSV' >> beam.Map(parse_lines)
        | 'ConvertTimestamp' >> beam.ParDo(convert_timestamp('%Y-%m-%d %H:%M:%S %Z'))
        | 'GreaterThan20' >> beam.ParDo(check_date())
        | 'From2010' >> beam.ParDo(check_value(20))
        | 'SumPerDate' >> beam.CombinePerKey(sum)
        # | 'ConvertToDict' >> beam.ParDo(convert_to_dict())
        # | 'ConvertResultToList' >> beam.combiners.ToList()
    )
    return a
