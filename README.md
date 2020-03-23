Serverless Athena
============

A plugin to manage athena table creation

## Installation

Install with **npm**:
```sh
npm install --save-dev serverless-athena
```

And then add the plugin to your `serverless.yml` file:
```yml
plugins:
  - serverless-athena
```

Alternatively, install with the Serverless **plugin command**:
```sh
sls plugin install -n serverless-athena
```

## Usage

in your `serverless.yml`
```yml
custom:
  athena:
    databases: # list of your dbs
      - name: my-db # your database name
        region: eu-west-1 # optional region
        output: s3://my-athena-output-bucket/ # required, your results bucket
        ddl: $(file(my-database.sql)} # optional, your DDL containing the CREATE DATABASE statement
        tables: # list of yout tables
          - name: mytable # Table name
            ddl: $(file(my-table.sql)} # required, DDL containing the CREATE TABLE
            keepPartitions: true # force backup and restore partitions
```

and

`my-table.sql`
```sql
CREATE EXTERNAL TABLE mytable (
  foo string,
  bar string
)
PARTITIONED BY (dt string)
ROW FORMAT  serde 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://${self:provider.environment.MY_BUCKET}/'
TBLPROPERTIES ('has_encrypted_data'='true');

```

