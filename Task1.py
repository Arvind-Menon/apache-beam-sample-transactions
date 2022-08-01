# -*- coding: utf-8 -*-
"""
Created on Mon Aug  1 07:20:26 2022

@author: Arvind Menon
"""

# =============================================================================
# Python script Task 1
# The DoFn functions are defined in the utils module.
# The code below show the flow of data in the pipeline, from reading and parsing the csv to export the results.
# The code currently can store the output in either csv or json format.

# Please Note: apache-beam[gcp] could not be installed on the local machine, 
# therefore the csv downloaded from `gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv`
# using pandas and store in './input/virgin_sample.csv'. The script's name read_gs.py

# To run using command prompt: python Task2.py --input ./input/virgin_sample.csv --output output.json.gz --runner DirectRunner
# =============================================================================

import argparse
import apache_beam as beam
from utils import *
import json


def run(argv = None):
    
    # parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--input")
    parser.add_argument("--output")
    args,beam_args = parser.parse_known_args(argv)
    
    with beam.Pipeline(argv = beam_args) as p:
        # read and parse csv
        lines = (
                    p 
                    |"ReadCSV" >> beam.io.ReadFromText(args.input, skip_header_lines=1)
                    |'ParseCSV' >> beam.Map(parse_lines))
        # transformations to filter values from 2010 and having transaction amount > 20
        lines_ts = (
                    lines 
                    | 'ConvertTimestamp' >> beam.ParDo(convert_timestamp(date_format = '%Y-%m-%d %H:%M:%S %Z'))
                    | 'GreaterThan20' >> beam.ParDo(check_date(date_format = '%Y-%m-%d', date = '2010-01-01'))
                    | 'From2010' >> beam.ParDo(check_value(20)))
        
        # combining the filtered output by date and finding the total transactions
        result = (lines_ts|'SumPerDate' >> beam.CombinePerKey(sum))
        
        # understanding the output extension
        output_filename = args.output.split('.')
        suffix = '.'.join(output_filename[1:])
        
        # if the output argument has the csv extension
        if 'csv' in output_filename[1]:
            (result
                        |'FormatOut' >> beam.Map(lambda element: ",".join(map(str,element)))
                        |"WriteCsvOutput" >> beam.io.WriteToText(f'./output/{output_filename[0]}',
                                                                 file_name_suffix=f".{suffix}",
                                                                 header='Date, total_amount')
              )
         
        # for json extension
        elif 'json' in output_filename[1]:
            (result 
                        | 'ConvertToDict' >> beam.ParDo(convert_to_dict())
                        | 'ConvertToListOfDicts' >>beam.combiners.ToList()
                        | 'ConvertToJson' >> beam.Map(json.dumps)
                        |"WriteJsonOutput" >> beam.io.WriteToText(f'./output/{output_filename[0]}'
                                                                 ,file_name_suffix=f'.{suffix}')
            )
        else: 
            print('Wrong Extension provided.\nPlease provide one among ".json" or ".csv" extensions')
        
        # print the intermediate results
        result | beam.Map(print)
        
if __name__ == "__main__":
    run()
