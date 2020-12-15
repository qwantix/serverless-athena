Serverless Athena Plugin
============

Serverless plugin to manage and deploy AWS Athena table

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
      - name: my-db # required, your database name, do not use an existing database, will be dropped in deployement process
        output: s3://my-athena-output-bucket/ # required, your results bucket
        ddl: $(file(my-database.sql)} # optional, your DDL containing the CREATE DATABASE statement
        sequential: true #optional (default: false), true if you want to create tables sequentially in case of dependencies between tables and views
        tables: # list of yout tables
          - name: mytable # required, table name
            ddl: $(file(my-table.sql)} # required, DDL containing the CREATE TABLE
            keepPartitions: true # force backup and restore partitions
      - name: my-db2 # required, your database name, do not use an existing database, will be dropped in deployement process
        output: s3://my-athena-output-bucket/ # required, your results bucket
        ddl: $(file(my-database.sql)} # optional, your DDL containing the CREATE DATABASE statement
        enabled: false #optional (default: true), plugin will ignore this database creation
        tables: # list of yout tables
          - name: mytable # required, table name
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

## Workflow

1. if `keepPartition = true` backuping partitions
2. Drop database, **therefore, do not use a existing database**
3. Create database
4. Create tables
5. Restore partitions if `keepPartition = true`


