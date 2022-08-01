# -*- coding: utf-8 -*-
"""
Created on Sun Jul 31 15:04:45 2022

@author: Arvind Menon
"""
# =============================================================================
# Code to check if the json output is readable
# =============================================================================

import gzip
import json

with gzip.open("./output/output-00000-of-00001.jsonl.gz", "r") as f:
   data = f.read()
   j = json.loads (data.decode('utf-8'))
   print (type(j))