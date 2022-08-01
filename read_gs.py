# -*- coding: utf-8 -*-
"""
Created on Mon Aug  1 09:51:02 2022

@author: Arvind Menon
"""
# =============================================================================
# To download the csv and save in a directory

# command prompt: python read_gs.py --input gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv --output input/virgin_sample.csv
# =============================================================================
import pandas as pd
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("--input")
parser.add_argument("--output")

args = parser.parse_args()

df = pd.read_csv(args.input)
df.to_csv(args.output, index = False)
print('Downloaded and saved')

