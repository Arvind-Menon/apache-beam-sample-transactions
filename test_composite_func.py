# -*- coding: utf-8 -*-
"""
Created on Sun Jul 31 18:03:03 2022

@author: Arvind Menon
"""
# =============================================================================
# Python code to test the composite function in utils.py
# The script has taken excerpts from testing examples
# provided at: https://beam.apache.org/documentation/pipelines/test-your-pipeline/#testing-a-pipeline-end-to-end
# This is a simple unit test, and more complicated tests can be scripted after diving deeper.

# To run using command prompt: python test_composite_func.py
# =============================================================================


import apache_beam as beam
import unittest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from utils import * # to import the composite function

class CountTest(unittest.TestCase):

  def test_count(self):
    # Create a test pipeline.
    with TestPipeline() as p:
      # Create an input PCollection.
      input = p | beam.io.ReadFromText('./input/unit test/virgin_unit_test1.csv', skip_header_lines=1)
      
      # Expected output
      expected_output = [('2010-01-01', 255), ('2017-01-01', 169), ('2017-03-18', 315)]
      
      # Output from the composite transform
      output = input | MyTransform()

      # Assert on the results.
      assert_that(
        output,
        equal_to(expected_output))


if __name__ == '__main__':
    print('Running Unit test for the composite function')  
    unittest.main()
  