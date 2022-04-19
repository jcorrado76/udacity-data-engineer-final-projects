This is the repo containing the 

To install Airflow into my local virtual environment using PyPI so that I can have the intelligent
IDE settings was:
```bash
pip install "apache-airflow[celery]==2.2.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.2.3/constraints-3.6.txt"
```

The documentation on how to use default arguments can be found [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html#default-arguments).

To complete the project, I need to implement four operators.

Those four operators are:
1. stage the data from s3 to redshift
2. load data into songplays fact table
3. load data into dimension tables
4. run data quality checks

Note that: If the S3 bucket that holds the data files doesn't reside in the same AWS Region as your cluster, you must use the REGION parameter to specify the Region in which the data is located.
Read more [here](https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-source-s3.html#copy-parameters-data-source-s3-examples)
## Stage Operator
We need to load any JSON formatted file from S3 to Redshift.
The operator should create and run a SQL COPY statement based on the parameters provided.

The parameters should specify:
* S3 bucket
* S3 key (use templating to partition file loading based on execution time)
* target table
## Fact and Dimension Operator
Use the SQL helper class to run the data transformations against your redshift cluster.

The operator should take in an input SQL statement, and a target table on which to run the query against.

You can also take in a target table name that will hold the result of the transformation.
## Data Quality Operator
The data quality operator is going to receive one or more SQL test cases, along with the expected results. 
The operator should execute the tests, and for each test, check for a match against the expected output.
If there is no match, the operator should raise an exception, and the task should retry and fail eventually.
