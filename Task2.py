# -*- coding: utf-8 -*-
"""
Created on Mon Aug  1 08:16:06 2022

@author: Arvind Menon
"""

# =============================================================================
# Python script for Task 2
# The composite function has been defined in the utils module under the name MyTransform()
# The code currently can store the output in either csv or json format. 
# Therefore the final transformations specifically required to export in these formats have not been added to the composite function.
# Error and exception handling have not been done to this script.

# Please Note: apache-beam[gcp] could not be installed on the local machine, 
# therefore the csv downloaded from `gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv`
# using pandas and stored in './input/virgin_sample.csv'. The script's name read_gs.py

# To run using command prompt: python Task2.py --input ./input/virgin_sample.csv --output output.json.gz --runner DirectRunner
# =============================================================================

import apache_beam as beam
from datetime import datetime
import json
from utils import *
import argparse
    
def run(argv=None):
    
    # parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--input")
    parser.add_argument("--output")
    args,beam_args = parser.parse_known_args(argv)
    
    # pipeline context manager
    with beam.Pipeline(argv = beam_args) as p:
        # read the input csv
        lines = (
                p 
                |"ReadCSV" >> beam.io.ReadFromText(args.input,
                                                   skip_header_lines=1))
        # the intermediate result from the composite transform function
        result =(
                lines 
                | 'IntermediateResult' >> MyTransform()
                )
        
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