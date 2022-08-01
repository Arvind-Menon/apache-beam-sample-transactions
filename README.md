# apache-beam-sample-transactions
First Apache-beam project to read sample data, perform transformations, and export the results
## Install libraries
To install the required libraries run the below command 
```sh
pip install -r requirements.txt
```
## Please Note:
apache-beam[gcp] could not be installed on the local machine.
Therefore a copy of the sample data was downloaded using pandas and stored in the input directory.
```sh
python read_gs.py --input gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv --output input/virgin_sample.csv
```
The task scripts should ideally work with the link, however the same could not be tested.
The same can be done using the command.
The code has not been widely tested, and exceptions/error handling has not been performed.
The output will be stored in the output directory. Currently, since all files, except .gitkeep, are ignored, no files exist in the output directory.
## Task1
To execute the code on command prompt run
##### with local csv:

```sh
python Task1.py --input ./input/virgin_sample.csv --output output.csv --runner DirectRunner
```
##### From google storage:

```sh
python Task1.py --input gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv --output output.csv --runner DirectRunner
```
## Task2
To execute the code on command prompt run
##### with local csv:

```sh
python Task2.py --input ./input/virgin_sample.csv --output output.csv --runner DirectRunner
```
##### From google storage:

```sh
python Task2.py --input gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv --output output.csv --runner DirectRunner
```
## Unit Test
To run the unit test on the composite transformation function execute:
```sh
python test_composite_func.py
```
## Contact
For clarification, error reporting, or any suggestions please email arvind.ramesh.menon@gmail.com
