/* eslint-disable no-shadow */

function getExecutor({
  provider,
  database,
  output,
  workgroup,
}) {
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

    // console.log('\nSQL', sql, '\n');
    const {
      QueryExecutionId,
    } = await provider.request('Athena', 'startQueryExecution', params);
    let waitTime = 0;
    const wait = async () => {
      waitTime = Math.min(1000, waitTime + 100);
      const res = await provider.request('Athena', 'getQueryExecution', {
        QueryExecutionId,
      });
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
          console.log(`FAILED QUERY ID ${QueryExecutionId} with params ${JSON.stringify(params)}`)
          console.log(`RES: ${res}`)
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
    this.provider = this.serverless.getProvider('aws');
  }

  get config() {
    const { athena } = this.serverless.service.custom || {};
    const databases = [];
    if (athena) {
      if (!(athena.databases instanceof Array)) {
        throw new Error('custome.athena.databases must be an array of databases');
      }
      athena.databases.forEach((config) => {
        const db = {
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
    let glueTable;
    try {
      const {
        Table,
      } = await this.provider.request('Glue', 'getTable', {
        DatabaseName: database,
        Name: table,
      });
      glueTable = Table;
    } catch (e) {
      const code = e.providerError ? e.providerError.code : e.code;
      if (code === 'EntityNotFoundException') {
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
      } = await this.provider.request('Glue', 'getPartitions', {
        DatabaseName: database,
        TableName: table,
        NextToken: nextToken,
        MaxResults: 1000,
      });
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
    let query = `ALTER TABLE ${table} ADD IF NOT EXISTS `;
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
        provider: this.provider,
        database: d.name,
        workgroup: d.workgroup,
        output: d.output,
      });

      // Backuping partitions in serie
      await d.tables
        .filter(t => t.keepPartitions)
        .reduce((p, t) => p.then(() => this.backupPartitions(d.name, t.name)), Promise.resolve());

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
        provider: this.provider,
        database: d.name,
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
