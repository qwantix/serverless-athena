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
    catalog: AwsDataCatalog #optional (default: AwsDataCatalog)
    databases: # list of your dbs
      - name: my-db # required, your database name, do not use an existing database, will be dropped in deployement process
        catalog: AwsDataCatalog #optional (default: AwsDataCatalog)
        output: s3://my-athena-output-bucket/ # required, your results bucket
        description: My demo database # optional, table comment
        properties: # optional, db properties
          - author: Me
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


## TODO

- [ ] Doing dryrun pre-deployement on tmp db to validate all ddl
- [ ] Backup partitions in file to manualy restore it if process fail
- [x] Update only tables or db if updated
- [x] Use default config values for all db (output, ...)
- [x] Data catalog support
- [ ] Allow to execute raw sql like add partition

From @PauloCarneiro99 :
- [x] Enable / Disable database or table from sls
- [x] Sequential deployement by default
- [x] Remove previously deployed database if not present



# Changelog
v2.0.0
- Removing custom ddl on database
