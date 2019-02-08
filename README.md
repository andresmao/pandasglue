
<p align="center">
  <img src="https://github.com/andresmao/test/blob/master/pandas_glue_logo_2.png" width="450" title="Pandas Glue">
</p>

# PandasGlue

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

[Amazon Glue](https://aws.amazon.com/glue/) is an [AWS](https://aws.amazon.com/) simple, flexible, and cost-effective ETL service and [Pandas](https://pandas.pydata.org/) is a Python library which provides high-performance, easy-to-use data structures and data analysis tools.

The goal of this package is help data engineers in the usage of cost efficient serverless compute services ([Lambda](https://aws.amazon.com/glue/), [Glue](https://aws.amazon.com/lambda/), [Athena](https://aws.amazon.com/athena/)) in order to provide an easy way to integrate Pandas with  AWS Glue,  allowing load the content of a DataFrame (**Write function**) directly in a table (parquet format) in the [Glue Data Catalog](https://docs.aws.amazon.com/glue/latest/dg/populate-data-catalog.html) and also execute Athena queries (**Read function**) returning the result directly in a Pandas DataFrame.

## Use cases

This package is recommended for ETL purposes which loads and transforms small to medium size datasets without requiring to create Spark jobs, helping reduce infrastructure costs.

It could be used within [Lambda functions](https://docs.aws.amazon.com/lambda/latest/dg/lambda-introduction-function.html), [Glue scripts](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python.html), [EC2](https://aws.amazon.com/ec2/) instances or any other infrastucture resources.

<p align="center">
  <img src="https://github.com/andresmao/test/blob/master/Pandas_glue_workflow2.png" width="700"  title="ETL Workflow">
</p>

### Prerequisites

```
pip install pandas
pip install boto3
pip install pyarrow 
```

### Installing the package

```
pip install pandasglue
```

## Usage 

**Read method:**

***read_glue()***

To retrieve the result of an Athena Query in a Pandas DataFrame.

Quick example:

```python
import pandas as pd
import pandasglue as pg

#Parameters
sql_query = "SELECT * FROM table_name LIMIT 20" 
db_name = "DB_NAME"
s3_output_bucket = "s3://bucket-url/"

df = pg.read_glue(sql_query,db_name,s3_output_bucket)

print(df)

```
query, db, s3_output, region=None, key=None, secret=None, profile_name=None

***Parameters list:***

* query: the SQL statement on Athena
* db: database name
* s3_output: S3 path for write query results (optional)
* key: AWS access key (optional)
* secret: AWS secret key (optional)
* profile_name: AWS IAM profile name (optional)


**Write method:**

***write_glue()***

Convert a given Pandas Dataframe to a Glue Parquet table.

Quick example:

```python
import pandas as pd
import pandasglue as pg

#Parameters
database = "DB_NAME"
table_name = "TB_NAME"
s3_path = "s3://bucket-url/"

#Sample DF
source_data = {'name': ['Sarah', 'Renata', 'Erika', 'Fernanda', 'Diana'], 
        'city': ['Seattle', 'Sao Paulo', 'Seattle', 'Santiago', 'Lima'],
         'test_score': [82, 52, 56, 234, 254]}
         
df = pd.DataFrame(source_data, columns = ['name', 'city', 'test_score'])


pg.write_glue(df, database, table_name, s3_path, partition_cols=['city'])


```
***Parameters list:***

* Param 1: explanation
* Param 2: explanation
* Param 3: explanation
* Param 4: explanation


## Built With

* [Boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) - (AWS) SDK for Python, which allows Python developers to write software that makes use of Amazon services like S3 and EC2.
* [PyArrow](https://pypi.org/project/pyarrow/) - Python package to interoperate Arrow with Python allowing to convert text files format to parquet files among other functions.


## Examples on AWS services:

Text here

## Contributing

Please read [CONTRIBUTING.md](https://gist.github.com/) for details on our code of conduct, and the process for submitting pull requests to us.

## Authors

* **Contributor 1* - *Initial work* - [Profile link](https://github.com/PurpleBooth)
* **Contributor 2* - *Initial work* - [Profile link](https://github.com/PurpleBooth)
* **Contributor 3* - *Initial work* - [Profile link](https://github.com/PurpleBooth)

See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Hat tip to anyone whose code was used
* Inspiration
* etc
