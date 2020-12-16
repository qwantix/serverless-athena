/* eslint-disable no-shadow */

const crypto = require('crypto')

// Return hash of str
function hash(str) {
  return crypto.createHash('md5').update(str).digest('hex')
}


function getExecutor({
  provider,
  catalog,
  database,
  output,
  workgroup,
}) {
  return async (sql) => {
    const params = {
      QueryString: sql,
      QueryExecutionContext: {
        Database: database,
        Catalog: catalog,
      },
      ResultConfiguration: {
        OutputLocation: output,
      },
      WorkGroup: workgroup,
    };

    console.log('\nSQL', sql, '\n');
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
          throw new Error(`Query failed: ${res.QueryExecution.Status.StateChangeReason}`);
        case 'CANCELLED':
          throw new Error('Query as been cancelled');
        default:
          throw new Error('Unexpected status code');
      }
    };
    await wait();
  };
}

// Escape single quote with double single quote
function ddlStringEscape(str) {
  return String(str).replace(/'/g, `''`);
}

// Generate properties map
function ddlPropertiesMap(properties) {
  return Object.keys(properties || {}).map((k) => {
    return `'${k}' = '${ddlStringEscape(properties[k])}'`
  }).join(',')
}

// Get Database DDL
function getDbDDL(dbConfig) {
  if (dbConfig.customDdl) {
    return dbConfig.ddl
  }

  // Build ddl
  let ddl = `CREATE DATABASE \`${dbConfig.name}\``;
  if (dbConfig.comment) {
    ddl += ` COMMENT '${ddlStringEscape(dbConfig.description)}'`
  }
  if (dbConfig.location) {
    ddl += ` LOCATION '${dbConfig.location}'`
  }
  const propMap = ddlPropertiesMap(dbConfig.properties)
  if (propMap) {
    ddl += ` WITH DBPROPERTIES (${propMap})`
  }
  return ddl;
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
    const {
      athena
    } = this.serverless.service.custom || {};

    // console.log(JSON.stringify(this.serverless.service, null, 2))
    const databases = [];
    if (athena) {
      if (!(athena.databases instanceof Array)) {
        throw new Error('custom.athena.databases must be an array of databases');
      }

      const defaults = {
        catalog: athena.catalog || 'AwsDataCatalog',
        workgroup: athena.workgroup || 'primary',
        output: athena.output,
      };

      athena.databases.forEach((config) => {
        if (config.enabled === false) {
          return
        }
        const db = {
          catalog: config.catalog || defaults.catalog,
          name: config.name || config.database,
          workgroup: config.workgroup || defaults.workgroup,
          output: config.output || defaults.output,
          properties: config.properties || {},
          description: config.description,
          tables: [],
        };

        if (!db.output) {
          throw new Error("'output' is empty");
        }

        if (config.tables && !(config.tables instanceof Array)) {
          throw new Error("'tables' must be an array");
        }

        db.properties = {
          ...db.properties,
          'sls.athena.creator': 'serverless-athena',
          'sls.athena.stack': this.provider.naming.getStackName(),
          'sls.athena.tables': config.tables.map(t => t.name).join(',')
        };

        db.ddl = getDbDDL(db);

        db.tables = (config.tables || [])
          .filter((t) => t.enabled !== false)
          .map((t) => {
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
            out.catalog = db.catalog;
            out.database = db.name;
            out.fullname = `${db.name}.${name}`;
            out.name = name;
            out.ddl = ddl;
            out.keepPartitions = keepPartitions === false ? false : true;
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

  async listDataCatalogs() {
    const list = []
    let nextToken = null
    do {
      const res = await this.provider.request('Athena', 'listDataCatalogs', {
        NextToken: nextToken,
      });
      list.push(...res.DataCatalogsSummary);
      nextToken = res.nextToken;
    } while (nextToken)
    return list
  }

  async listDatabases(catalog) {
    const list = []
    let nextToken = null
    do {
      const res = await this.provider.request('Athena', 'listDatabases', {
        CatalogName: catalog,
        NextToken: nextToken,
      });
      list.push(...res.DatabaseList);
      nextToken = res.nextToken;
    } while (nextToken)
    return list
  }

  async getDatabase(catalog, name) {
    try {
      const {
        Database
      } = await this.provider.request('Athena', 'getDatabase', {
        CatalogName: catalog,
        DatabaseName: name
      });
      return Database
    } catch (e) {
      if (e.providerError && /Error Code: EntityNotFoundException/.test(e.providerError.message)) { // XXX Better idea than regex on message ?
        return null
      }
      throw e
    }
  }

  async removeDatabase(name) {
    this.log(`${name}: removing database`);
    await this.provider.request('Glue', 'deleteDatabase', {
      Name: name
    })
  }

  async createDatabase(executor, config) {
    this.log(`${config.name}: creating database`);
    return executor(config.ddl);
  }

  async updateDatabase(dbConfig, parameters) {
    this.log(`${dbConfig.name}: updating parameters`)
    const db = await this.getDatabase(dbConfig.catalog, dbConfig.name)
    await this.provider.request('Glue', 'updateDatabase', {
      Name: dbConfig.name,
      DatabaseInput: {
        Name: dbConfig.name,
        Description: dbConfig.description,
        LocationUri: dbConfig.location,
        Parameters: {
          ...db.Parameters,
          ...parameters
        }
      }
    })
  }

  async listTables(catalog, database) {
    const list = []
    let nextToken = null
    do {
      const res = await this.provider.request('Athena', 'listTableMetadata', {
        CatalogName: catalog,
        DatabaseName: database,
        NextToken: nextToken,
      });
      list.push(...res.TableMetadataList);
      nextToken = res.nextToken;
    } while (nextToken)
    return list
  }

  async getTable(catalog, database, table) {
    try {
      const {
        TableMetadata
      } = await this.provider.request('Athena', 'getTableMetadata', {
        CatalogName: catalog,
        DatabaseName: database,
        TableName: table,
      });
      return TableMetadata
    } catch (e) {
      console.log(e)
      if (e.providerError && /Error Code: EntityNotFoundException/.test(e.providerError.message)) { // XXX Better idea than regex on message ?
        return null
      }
      throw e
    }
  }


  async removeTable(executor, name) {
    this.log(`${name}: removing table`);
    return executor(`DROP TABLE IF EXISTS ${name}`);
  }

  async createTable(executor, name, ddl) {
    this.log(`${name}: creating table`);
    return executor(ddl);
  }

  async tableUpdated(config, table) {
    if (!table) {
      return true
    }
    return hash(config.ddl) !== table.Parameters['sls.athena.hash']
  }

  async setTableParameters(executor, tableConfig, parameters) {
    this.log(`${tableConfig.fullname}: updating parameters`)
    const query = `ALTER TABLE ${tableConfig.fullname} SET TBLPROPERTIES (${
      Object.keys(parameters).map((k)=>{
        return `'${k}' = ${JSON.stringify(parameters[k])}`
      }).join(',')
    });`
    return executor(query)
  }

  async backupPartitions(catalog, database, tableName) {
    this.log(`${database}.${tableName}: backuping partitions`);

    const table = await this.getTable(catalog, database, tableName)

    const cols = table.PartitionKeys.map(p => p.Name);
    const partitions = [];

    const grabPartitions = async (nextToken) => {
      const {
        NextToken,
        Partitions,
      } = await this.provider.request('Glue', 'getPartitions', {
        DatabaseName: database,
        TableName: tableName,
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

    this.log(`${database}.${tableName}: ${partitions.length} partitions backuped`);
    this.partitions.set(`${database}.${tableName}`, partitions);
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

  async deployTable(executor, tableConfig, table) {
    this.log(`${tableConfig.fullname}: deploy`);
    const hasPartitions = table && table.PartitionKeys && table.PartitionKeys.length > 0
    if (hasPartitions && tableConfig.keepPartitions) {
      // Backup partition only if is partionned
      await this.backupPartitions(tableConfig.catalog, tableConfig.database, tableConfig.name);
    }
    const needRemove = await this.tableUpdated(tableConfig, table);
    if (needRemove) {
      await this.removeTable(executor, tableConfig.name);
    }
    if (!table || needRemove) {
      await this.createTable(executor, tableConfig.name, tableConfig.ddl);
    }
    await this.setTableParameters(executor, tableConfig, {
      'sls.athena.hash': hash(tableConfig.ddl),
      'sls.athena.stack': this.provider.naming.getStackName(),
    })

    const newTable = await this.getTable(tableConfig.catalog, tableConfig.database, tableConfig.name);
    const newTablePartionned = newTable.PartitionKeys && newTable.PartitionKeys.length > 0;
    if (newTablePartionned) {
      // Restore partition if new table is partionned
      await this.restorePartitions(executor, tableConfig.database, tableConfig.name)
    }
    this.log(`${tableConfig.fullname}: done`);
  }

  async deployDatabase(d) {
    this.log('Deploy database:', d.name);
    const dbExecutor = getExecutor({
      catalog: d.catalog,
      provider: this.provider,
      workgroup: d.workgroup,
      output: d.output,
    });

    const tableExecutor = getExecutor({
      catalog: d.catalog,
      provider: this.provider,
      database: d.name,
      workgroup: d.workgroup,
      output: d.output,
    });

    const db = await this.getDatabase(d.catalog, d.name);

    if (!db) {
      await this.createDatabase(dbExecutor, d);
    } else {
      await this.updateDatabase(d, {});
    }

    const tables = await this.listTables(d.catalog, d.name)

    for (let tableConfig of d.tables) {
      const table = tables.find(t => t.Name === tableConfig.name);
      await this.deployTable(tableExecutor, tableConfig, table)
    }

    this.removeTrailingTables(tableExecutor, tables, d.tables)
    this.log('Leaving deploy');
  }

  // Remove previously deployed database
  async removeTrailingDatabases(usedDatabases) {
    const stack = this.provider.naming.getStackName();
    this.log('Searching trailing databases for stack: ' + stack)

    const catalogs = await this.listDataCatalogs()

    for (let c of catalogs) {
      const databases = await this.listDatabases(c.CatalogName);
      const usedsSet = new Set(usedDatabases.map(d => d.name));
      const trailings = databases
        .filter(d => d.Parameters &&
          d.Parameters['sls.athena.stack'] === stack &&
          !usedsSet.has(d.Name))
      this.log(`Found ${trailings.length} trailing database in catalog ${c.CatalogName}`);
      for (let d of trailings) {
        await this.removeDatabase(d.Name)
      }
    }
  }

  async removeTrailingTables(executor, tables, usedTables) {
    const stack = this.provider.naming.getStackName();
    this.log('Searching trailing tables for stack: ' + stack)

    const usedSet = new Set(usedTables.map(t => t.name));
    const trailings = tables
      .filter(t => t.Parameters &&
        t.Parameters['sls.athena.stack'] === stack &&
        !usedSet.has(t.Name))
    this.log(`Found ${trailings.length} trailing tables`);
    for (let t of trailings) {
      await this.removeTable(executor, t.Name)
    }

  }

  async deploy() {
    const {
      databases,
    } = this.config;
    this.log('Entering deploy');


    for (let db of databases) {
      await this.deployDatabase(db)
    }

    await this.removeTrailingDatabases(databases)

    this.log('Leaving deploy');
  }

  async remove() {
    const {
      databases,
    } = this.config;
    this.log('Entering remove');
    await Promise.all(databases.map(async (d) => {
      await this.removeDatabase(d);
    }));
    this.log('Leaving remove');
  }

  log(...args) {
    this.serverless.cli.log(args.join(' '), 'Serverless Athena Plugin');
  }
}

module.exports = ServerlessAthenaPlugin;