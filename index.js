/* eslint-disable no-shadow */


const aws = require('aws-sdk');

function getExecutor({
  region,
  database,
  output,
  workgroup,
}) {
  const athena = new aws.Athena({
    region,
  });

  return async (sql) => {
    const params = {
      QueryString: sql,
      QueryExecutionContext: {
        Database: database,
      },
      ResultConfiguration: {
        OutputLocation: output,
      },
      WorkGroup: workgroup,
    };

    // console.log('\nSQL', sql, '\n')

    const {
      QueryExecutionId,
    } = await athena.startQueryExecution(params).promise();
    let waitTime = 0;
    const wait = async () => {
      waitTime = Math.min(1000, waitTime + 100);
      const res = await athena.getQueryExecution({
        QueryExecutionId,
      }).promise();
      switch (res.QueryExecution.Status.State) {
        case 'QUEUED':
        case 'RUNNING':
          return new Promise(($resolve) => {
            setTimeout(async () => {
              await wait();
              $resolve();
            }, waitTime);
          });
        case 'SUCCEEDED':
          return Promise.resolve();
        case 'FAILED':
          throw new Error('Query failed');
        case 'CANCELLED':
          throw new Error('Query as been cancelled');
        default:
          throw new Error('Unexpected status code');
      }
    };
    await wait();
  };
}


class ServerlessAthenaPlugin {
  constructor(serverless, options) {
    this.serverless = serverless;
    this.options = options;
    this.hooks = {
      'after:deploy:deploy': this.deploy.bind(this),
      'remove:remove': this.remove.bind(this),
    };
    this.partitions = new Map();
  }

  get config() {
    const { athena } = this.serverless.service.custom || {};
    const databases = [];
    if (athena) {
      if (!(athena.databases instanceof Array)) {
        throw new Error('custome.athena.databases must be an array of databases');
      }
      const { region } = this.serverless.service.provider;
      athena.databases.forEach((config) => {
        const db = {
          region: config.region || region,
          name: config.name || config.database,
          workgroup: config.workgroup || 'primary',
          output: config.output,
          ddl: config.ddl,
          existing: !!config.existing,
          tables: [],
        };

        if (!config.output) {
          throw new Error("'output' is empty");
        }

        if (config.tables && !(config.tables instanceof Array)) {
          throw new Error("'tables' must be an array");
        }

        db.tables = (config.tables || []).map((t) => {
          const out = {};
          const {
            name,
            region,
            workgroup,
            ddl,
            keepPartitions,
          } = t;

          if (!name) {
            throw new Error('Invalid name');
          }
          if (!ddl) {
            throw new Error('Invalid DDL');
          }
          out.fullname = `${db.name}.${name}`;
          out.name = name;
          out.region = region || db.region;
          out.ddl = ddl;
          out.keepPartitions = !!keepPartitions;
          out.workgroup = workgroup || db.workgroup;
          return out;
        });
        databases.push(db);
      }, databases);
    }

    return {
      databases,
    };
  }


  async removeDatabase(executor, config) {
    this.log(`${config.name}: removing database`);
    return executor(`DROP DATABASE IF EXISTS \`${config.name}\` CASCADE`);
  }

  async createDatabase(executor, config) {
    this.log(`${config.name}: creating database`);
    const ddl = config.ddl || `CREATE DATABASE \`${config.name}\`
        WITH DBPROPERTIES('creator' = 'serverless-athena');
    `;
    return executor(ddl);
  }

  async removeTable(executor, config) {
    this.log(`${config.fullname}: removing table`);
    return executor(`DROP TABLE IF EXISTS ${config.name}`);
  }

  async createTable(executor, config) {
    this.log(`${config.fullname}: creating table`);
    return executor(config.ddl);
  }


  async backupPartitions(database, table) {
    this.log(`${database}.${table}: backuping partitions`);
    const glue = new aws.Glue({
      region: this.config.databases.find(d => d.name === database).region,
    });
    let glueTable;
    try {
      const {
        Table,
      } = await glue.getTable({
        DatabaseName: database,
        Name: table,
      }).promise();
      glueTable = Table;
    } catch (e) {
      if (e.code === 'EntityNotFoundException') {
        return; // Table doesn't exists
      }
      throw e;
    }

    const cols = glueTable.PartitionKeys.map(p => p.Name);
    const partitions = [];

    const grabPartitions = async (nextToken) => {
      const {
        NextToken,
        Partitions,
      } = await glue.getPartitions({
        DatabaseName: database,
        TableName: table,
        NextToken: nextToken,
        MaxResults: 1000,
      }).promise();
      partitions.push(...Partitions.map(p => ({
        values: cols.map((c, i) => ({
          name: c,
          value: p.Values[i],
        })),
        location: p.StorageDescriptor.Location,
      })));

      if (NextToken) {
        await grabPartitions(NextToken);
      }
    };

    await grabPartitions();

    this.log(`${database}.${table}: ${partitions.length} partitions backuped`);
    this.partitions.set(`${database}.${table}`, partitions);
  }

  async restorePartitions(executor, database, table) {
    const partitions = this.partitions.get(`${database}.${table}`) || [];

    this.log(`${database}.${table}: restoring ${partitions.length} partitions`);
    if (!partitions.length) return;
    let query = `ALTER TABLE ${table} ADD `;
    query += partitions
      .map(({
        values,
        location,
      }) => {
        const cols = values.map(({
          name,
          value,
        }) => `\`${name}\` = '${value}'`);
        return `PARTITION (${cols}) LOCATION '${location}'`;
      })
      .join(' ');

    await executor(query);
  }

  async deploy() {
    const {
      databases,
    } = this.config;
    this.log('Entering deploy');
    return Promise.all(databases.map(async (d) => {
      this.log('Found database:', d.name);
      const executor = getExecutor({
        database: d.name,
        region: d.region,
        workgroup: d.workgroup,
        output: d.output,
      });

      // Backuping partitions
      await Promise.all(
        d.tables
          .filter(t => t.keepPartitions)
          .map(t => this.backupPartitions(d.name, t.name)),
      );
      await this.removeDatabase(executor, d);
      await this.createDatabase(executor, d);
      await Promise.all(
        d.tables
          .map(t => this.createTable(executor, t)),
      );

      // Restoring partitions
      await Promise.all(
        d.tables
          .filter(t => t.keepPartitions)
          .map(t => this.restorePartitions(executor, d.name, t.name)),
      );

      this.log('Leaving deploy');
    }));
  }

  async remove() {
    const {
      databases,
    } = this.config;
    this.log('Entering remove');
    return Promise.all(databases.map(async (d) => {
      const executor = getExecutor({
        database: d.name,
        region: d.region,
        workgroup: d.workgroup,
        output: d.output,
      });
      await this.removeDatabase(executor, d);
      this.log('Leaving remove');
    }));
  }

  log(...args) {
    this.serverless.cli.log(args.join(' '), 'Serverless Athena Plugin');
  }
}

module.exports = ServerlessAthenaPlugin;
