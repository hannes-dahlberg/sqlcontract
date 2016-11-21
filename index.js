var fs = require('fs-extra');
var mkdirp = require('mkdirp');
var sql = require('mssql');
var Promise = require('bluebird');
var _ = require('lodash');
var firstBy = require('thenby');

var Prom = require('./helpers/Prom');
var Arr = require('./helpers/Arr');
var Obj = require('./helpers/Obj');
var Str = require('./helpers/Str');

//Constructor
function SQL() { }

SQL.prototype = {
    connection: false,
    /**
     * Creating a connection to an SQL database
     * @param connectionProperties Can be connection string as well
     */
    connect: function(connectionProperties) {
        return new Promise((resolve, reject) => {
            var connection = new sql.Connection(connectionProperties, (error) => {
                if(error) { reject(error); return; }
                this.connection = connection;
                resolve(connection);
            });
        });
    },
    /**
     * Querying a connection with provided options
     * @param query
     * @param options
     */
    query: function(query, options) {
        return new Promise((resolve, reject) => {
            //Set options to empty object if not defined
            if(typeof options == 'undefined') { options = {}; }

            /*Since the query method can be called multiple times with array of queries and other
            whatnots each query is entitled to provide it's own set of options as well. This is useful
            when you want to have the query result in different format for different queries or tie a
            specific query to its own connection. The options parameter will be merged with (if exists)
            the options parameter in the query object. The merge will only overwrite provided options
            and leave all other options intact*/
            if(typeof query.options != 'undefined') {
                options = Obj.merge(options, query.options);
                /*Clear out options from the query so they are not provided again if method is
                calling itself before actually executing the query*/
                query.options = {};
            }

            //Set to use the current connection
            var connection = this.connection;

            //Connection can also be provided as an option
            if(Obj.getType(options.connection) != undefined && options.connection !== false) {
                if(Obj.getType(options.connection) == 'Connection') {
                    //If the connection options is already an established connection, set to use it
                    connection = options.connection;
                } else if (['Object', 'String'].indexOf(Obj.getType(options.connection)) != -1) {
                    /*The connection can also be config options or a connection string. If so establish
                    a connection, update options object and call itself again*/
                    this.connect(options.connection).then((connection) => {
                        //Saving the connection to options
                        options.connection = connection;
                        //Calling itself again but now with an established connection
                        this.query(query, options).then((recordSets) => {
                            resolve(recordSets);
                        }).catch((error) => reject(error));
                    }).catch((error) => reject(error));

                    //End method
                    return;
                }
            }

            /*Rejects if no connection has been established
             Make sure to run the connect method first*/
            if(!connection) { reject(new Error('No connection was found')); }

            //Look for transaction object in options
            if(typeof options.transaction != 'undefined' && Obj.getType(options.transaction) == 'Transaction') {
                //Set the connection to the transaction object instead
                connection = options.transaction;
            }

            /*Checks if the transaction option is set to true. If so
             create a new transaction and wrap query inside it*/
            if(typeof options.transaction != 'undefined' && Obj.getType(options.transaction) == 'Boolean') {
                var transaction = new sql.Transaction(connection);
                transaction.begin((error) => {
                    if(error) { reject(error); return; }

                    //Set the transaction option to use the newly created transaction
                    options.transaction = transaction;
                    //call itself but now with the transaction object instead
                    this.query(query, options).then((recordSets) => {
                        //Commit the transaction after query has been processed
                        transaction.commit((error) => {
                            //Reject any commit error
                            if(error) { reject(error); return; }

                            //Resolve the query recordSets
                            resolve(recordSets);
                        });

                    }).catch((error) => { //Query error
                        //Rollback transaction
                        transaction.rollback((rollbackError) => {
                            //Reject with caught error
                            reject(error);
                        });
                    });
                });

                //End method
                return;
            }

            /*Get query data. Will return the query
             in format: { name: {String}, statement: {String} }
             if the query was provided as a string. If the query
             was already an object or an array it will be untouched*/
            query = SQL.queryData(query);

            //If a single table is provided add it to the tables array
            if(Obj.getType(query.table) != undefined) {
                query.tables = [query.table];

                //Delete table from query
                delete query.table;
            }
            /*Check if tables is provided in query and if so, preform
            a bulk using the current connection*/
            if(Obj.getType(query.tables) == 'Array') {
                //Creates a new request
                var request = new sql.Request(connection);

                if(typeof options.callback !='undefined') {
                    /*Callback with the complete query object (name, statement and table)
                     right before bulk insert*/
                    query.status = 'bulk';
                    options.callback(query);
                }

                //Holder for bulk promises
                var subs = [];
                query.tables.forEach((table) => {
                    //If trying to create a temporary table
                    if(Str.substr(table.name, 0, 1) == '#' && table.create) {
                        subs.push(() => {
                            return new Promise((resolve, reject) => {
                                //Remove (if exists) the temporary table before creating it
                                request.query('IF OBJECT_ID(\'tempdb..' + table.name + '\') IS NOT NULL BEGIN DROP TABLE ' + table.name + ' END', (error) => {
                                    if(error) { reject(error); return; }
                                    resolve();
                                });
                            });
                        });
                    }
                    //Add each table to a bulk request wrapped in a promise
                    subs.push(() => {
                        return new Promise((resolve, reject) => {
                            request.bulk(table, (error) => {
                                if(error) { reject(error); return; }

                                resolve();
                            });
                        });
                    });
                });

                //Execute all promises in sequence
                Prom.sequence(subs).then((results) => {
                    //Reject on any error
                    if(results.some(_.isError)) { resolve(results.filter(_.isError)); return; }

                    if(typeof options.callback !='undefined') {
                        /*Callback with the complete query object (name, statement and table)
                         after bulk insert is complete (without errors)*/
                        query.status = 'complete'
                        options.callback(query);
                    }

                    /*Delete the query table object because it shouldn't be added again when
                     executing the actual query*/
                    delete query.tables;

                    /*Call itself with the same query as before (but now without the table)
                     When executing the query the bulk inserted table will exists in the content
                     of the query. This makes great for adding temporary tables prior of execution*/
                    this.query(query, options).then((recordSets) => {
                        resolve(recordSets);
                    }).catch((error) => {
                        reject(error);
                    });
                });

                //End method
                return;
            }

            /*If batch option is set and the query is a string
             Use the batch option to separate query at each GO*/
            if(typeof options.batch != 'undefined' && options.batch && Obj.getType(query) == 'Object') {
                //Split the query on each GO with queryBatch
                var queries = SQL.queryBatch(query.statement);
                /*If the query was able to split on multiple GO, set
                 query to the result of the queryBatch (an array of
                 multiple queries)*/
                if(queries.length > 1) {
                    /*Each new query will inherit all data (except for
                     the statement property) from the original query
                     with the use of the Obj.merge method*/
                    query = queries.map((subQuery, index) => { return Obj.merge(query, { index: index, statement: subQuery }); });
                    /*Set the batch option to false. Since we later
                     will call this method (this.query) with multiple queries,
                     try to split them by GO again will have no effect,
                     thus unnecessary to even try*/
                    options.batch = false;
                }
            }

            /*If array of queries are provided (or the query was split with
             the query batch method)*/
            if(Obj.getType(query) == 'Array' || Obj.getType(query.queries) == 'Array') {
                //Holder for all query promises
                var subs = [];
                var queries = query;
                if(Obj.getType(query.queries) == 'Array') {
                    queries = query.queries;
                }
                queries.forEach((query) => {
                    //Push promise of single query to subs
                    subs.push(() => {
                        return new Promise((resolve, reject) => {
                            this.query(query, options)
                                .then((recordSets) => resolve(recordSets))
                                .catch((error) => reject(error));
                        });
                    });
                });

                //Multiple queries and callback
                if(Obj.getType(query.queries) == 'Array' && Obj.getType(query.callback) == 'Function') {
                    query.status = 'executing';
                    query.callback(query);
                }

                //Execute all query promises in sequence
                Prom.sequence(subs).then((recordSets) => {
                    //Reject if any of them has an error
                    if(recordSets.some(_.isError)) {
                        //Reject with first error
                        reject(recordSets.filter(_.isError)[0]);
                        return;
                    }


                    /*If multiple queries were provided as property and a callback property was provided
                    as well, the callback will be called when all queries has been executed*/
                    if(Obj.getType(query.queries) == 'Array' && Obj.getType(query.callback) == 'Function') {
                        query.status = 'complete';
                        query.callback(query);
                    }

                    //flatten result and resolve
                    var level = 1;
                    if(typeof options.fetchArray != 'undefined') { level = 0; }
                    resolve(Arr.flatten(recordSets, level));
                });

                //End method
                return;
            }

            //No transaction or multi queries, just a single SQL query by string
            var request = new sql.Request(connection);
            //Set to handel multiple record sets
            request.multiple = true;

            if(typeof options.callback !='undefined') {
                /*Callback with the complete query object (name and statement)
                right before executing the query*/
                query.status = 'executing';
                options.callback(query);
            }

            //Check if query result should be streamed or not
            if(Obj.getType(options.stream) != undefined && options.stream && Obj.getType(options.streamCallback) == 'Function') { //Stream result
                //Set stream for request
                request.stream = true;

                //Executing the query
                request.query(query.statement);

                //Invoke streamCallback function on each request callback
                request.on('recordset', (columns) => {
                    options.streamCallback('recordset', {
                        columns: columns,
                        query: query
                    });
                });

                request.on('row', (row) => {
                    options.streamCallback('row', {
                        row: row,
                        query: query
                    });
                });

                //If an error occur reject
                request.on('error', (error) => {
                    reject(error);
                });

                //When the request is complete resolve
                request.on('done', (returnValue, rowsAffected) => {
                    //Make callback if callback option exists
                    if(Obj.getType(options.callback) == 'Function') {
                        query.status = 'complete'
                        options.callback(query);
                    }
                    resolve({
                        returnValue: returnValue,
                        rowsAffected: rowsAffected
                    });
                });

                //End method
                return;
            }

            //No stream execute the query as usual
            request.query(query.statement, (error, recordSets) => {
                //Reject on SQL-error
                if(error) {
                    error.query = query;

                    reject(error);
                    return;
                }

                if(typeof options.callback !='undefined') {
                    /*Callback with the complete query object (name and statement)
                    after execution is complete (without errors)*/
                    query.status = 'complete'
                    options.callback(query);
                }

                /*If recordSet is empty (query executed but no record set could
                 be fetch), resolve and end*/
                if(typeof recordSets == 'undefined') {
                    resolve();
                    return;
                }

                //Fetch array (fields and rows in separate arrays)
                if(typeof options.fetchArray != 'undefined') {
                    //Update each record set with associated values
                    recordSets.forEach((recordSet, index) => recordSets[index] = SQL.fetchArray(recordSet));
                }
                //Add queryobject to recordSets array
                recordSets.forEach((recordSet, index) => recordSets[index].query = query);
                resolve(recordSets);
            });
        })
    },
    /**
     * Creating a new table object that can be used for bulk insert
     *
     * @param name - The table name, use # prefix for temp tables
     * @param columns - Columns in the same format as return from query (with fetchArray = false)
     * @param rows - Rows in the same forma as return from query
     * @returns {Table}
     */
    createTable: function(name, columns, rows) {
        var table = new sql.Table(name);
        table.create = true;
        Object.keys(columns).forEach((index) => {
            var options = {};
            Object.keys(columns[index]).forEach((columnIndex) => {
                if(['index', 'name', 'type', 'length'].indexOf(columnIndex) == -1) {
                    options[columnIndex] = columns[index][columnIndex];
                }
            });

            if(Obj.getType(columns[index].type) == undefined) {
                var columnType = sql.Float;
            } else {
                if(Obj.getType(columns[index].length != undefined)) {
                    var columnType = columns[index].type(columns[index].length);
                } else {
                    var columnType = columns[index].type;
                }
            }
            table.columns.add(columns[index].name, columnType, options);
        });
        rows.forEach((row) => {
            var rowValues = [];
            Object.keys(row).forEach((index) => {
                rowValues.push(row[index]);
            });

            table.rows.add.apply(table.rows, rowValues);
        });

        return table;
    },
    /**
     * Get dbo object files
     * @param options
     */
    dumpTable: function(table) {

    },
    getDbObjectFiles: function(options) {
        return new Promise((resolve, reject) => {
            //Options must be provided. Reject if not
            if (typeof options == 'undefined') { reject(new Error('No option parameter was provided')); return; }

            //Root option must be provided. Reject if not
            if (typeof options.directory == 'undefined') { reject(new Error('No root option was provided')); return; }
            //Set root properties
            var root = {
                schema: options.directory + '/schemas',
                procedure: options.directory + '/procedures',
                function: options.directory + '/functions',
                //table: options.directory + '/tables',
                //tableData: options.directory + '/table_data'
            };

            var dbObjects = [];
            Object.keys(root).forEach((index) => {
                dbObjects = dbObjects.concat(fs.readdirSync(root[index]).filter((file) => {
                    return Str.substr(file, -4, 4) == '.sql';
                }).map((file) => {
                    return {
                        name: Str.substr(file, 0, -4),
                        type: index,
                        path: root[index] + '/' + file
                    }
                }));
            });

            resolve(dbObjects);
        });
    },
    import: function(options) {
        return new Promise((resolve, reject) => {
            //Options must be provided. Reject if not
            if(typeof options == 'undefined') { reject(new Error('No option parameter was provided')); return; }

            //Root option must be provided. Reject if not
            if(typeof options.directory == 'undefined') { reject(new Error('No root option was provided')); return; }

            //Set root properties
            var root = {
                schema: options.directory + '/schemas',
                procedure: options.directory + '/procedures',
                function: options.directory + '/functions',
                //table: options.directory + '/tables',
                //tableData: options.directory + '/table_data'
            };

            this.getDbObjectFiles({
                directory: options.directory
            }).then((dbObjects) => {
                if(typeof options.dbObjects != 'undefined') {
                    //Filter dboObjects to only include dbObjects provided in options.dbObjects
                    dbObjects = dbObjects.filter((item) => options.dbObjects.some((subItem) => item.name == subItem.name));
                }

                //Sorting by type (will be ordered: schema, function, procedure)
                dbObjects.sort(firstBy((v1, v2) => {
                    var compareArray = ['schema', 'function', 'procedure'];
                    return compareArray.indexOf(v1.type) > compareArray.indexOf(v2.type);
                }));

                //Number of objects
                var total = {
                    procedures: dbObjects.filter((item) => item.type == 'procedure').length,
                    functions: dbObjects.filter((item) => item.type == 'function').length,
                    schemas: dbObjects.filter((item) => item.type == 'schema').length
                };
                total.all = dbObjects.length;


                if(typeof options.callback != 'undefined') {
                    options.callback({
                        info: 'init'
                    });
                }

                //Template query for deleting routine
                var deleteRoutineQuery = fs.readFileSync(__dirname + '/resources/sql/delete_routine.sql', 'utf8');

                //Holder for promises when collecting queries
                subs = [];

                //Options for clearing out all functions and procedures before importing
                if(typeof options.empty != 'undefined') {
                    subs.push(() => {
                        return new Promise((resolve, reject) => {
                            //Query for listing all db objects
                            var query = fs.readFileSync(__dirname + '/resources/sql/list_all.sql', 'utf8');
                            this.query(query).then((recordSets) => {
                                var dbObjects = recordSets[0];
                                //Holder for delete queries
                                var queries = [];
                                //Filter out only procedures and functions
                                dbObjects = dbObjects.filter((item) => ['procedure', 'function'].indexOf(item.type) >= 0);
                                //Query for deleting routine
                                dbObjects.forEach((dbObject) => {
                                    //Add delete query for each procedure and function
                                    queries.push({
                                        info: 'delete_object',
                                        dbObject: dbObject,
                                        statement: Str.replace(['%ROUTINE%', '%TYPE%'], [dbObject.name, dbObject.type.toUpperCase()], deleteRoutineQuery)
                                    });
                                });

                                //Resolve with all queries
                                if(queries.length > 0) {
                                    resolve(queries);
                                    return;
                                }

                                resolve();
                            }).catch((error) => reject(error));
                        });
                    });
                }

                //Template query for creating schema
                var schemaQuery = fs.readFileSync(__dirname + '/resources/sql/create_schema.sql', 'utf8');
                dbObjects.forEach((dbObject, index) => {
                    subs.push(() => {
                        return new Promise((resolve, reject) => {
                            //If dbobject is schema, use schema query template for statement
                            if(dbObject.type == 'schema') {
                                resolve({
                                    info: 'create_object',
                                    dbObject: dbObject,
                                    total: total.all,
                                    index: index + 1,
                                    statement: Str.replace(['%SCHEMA%'], [dbObject.name], schemaQuery)
                                });
                            } else { //functions and procedures queries
                                //Read dbObject file
                                fs.readFile(dbObject.path, 'utf8', (error, data) => {
                                    //Reject on file read error
                                    if(error) { reject(error); return; }

                                    var resolveObject = {
                                        info: 'create_object',
                                        dbObject: dbObject,
                                        total: total.all,
                                        index: index + 1,
                                        statement: data
                                    }

                                    /*If not all objects just has been deleted each
                                     db routine need to be deleted before being created*/
                                    if(typeof options.empty == 'undefined') {
                                        resolve([{
                                            info: 'delete_object',
                                            dbObject: dbObject,
                                            total: total.all,
                                            index: index + 1,
                                            statement: Str.replace(['%ROUTINE%', '%TYPE%'], [dbObject.name, dbObject.type.toUpperCase()], deleteRoutineQuery)
                                        }, resolveObject]);
                                    } else {
                                        //Resolve query object
                                        resolve(resolveObject);
                                    }

                                });
                            }
                        });
                    });
                });
                //Execute promises for getting all queries
                Prom.sequence(subs).then((results) => {
                    //Reject on any error
                    if(results.some(_.isError)) { reject(results.filter(_.isError)); return; }

                    var queries = results.filter((value) => typeof value != 'undefined');

                    //Option for resolving all queries instead of executing them
                    if(Obj.getType(options.export) != undefined && options.export) {
                        resolve(queries);
                        return;
                    }

                    //Execute all resolved queries from sequence within a transaction
                    this.query(queries, {
                        transaction: true,
                        callback: (data) => {
                            if(typeof options.callback != 'undefined') {
                                options.callback(data);
                            }
                        }
                    }).then(() => {
                        if(typeof options.callback != 'undefined') { options.callback({
                            info: 'done'
                        }); }
                    }).catch((error) => reject(error));
                });

            }).catch((error) => reject(error));
        });
    },
    export: function(options) {
        return new Promise((resolve, reject) => {
            //Options must be provided. Reject if not
            if(typeof options == 'undefined') { reject(new Error('No option parameter was provided')); return; }

            //Root option must be provided. Reject if not
            if(typeof options.directory == 'undefined') { reject(new Error('No root option was provided')); return; }

            //Set root properties
            var root = {
                schema: options.directory + '/schemas',
                procedure: options.directory + '/procedures',
                function: options.directory + '/functions',
                //table: options.directory + '/tables',
                //tableData: options.directory + '/table_data'
            };

            //Create dirs (if not existing)
            Object.keys(root).forEach(function(index) {
                mkdirp.sync(root[index]);

                //Option for emptying directories
                if(typeof options.emptyDirs != 'undefined' && options.emptyDirs) {
                    fs.emptyDirSync(root[index]);
                }
            });

            //query to list all db objects in database
            var query = fs.readFileSync(__dirname + '/resources/sql/list_all.sql', 'utf8');
            this.query(query).then((recordSets) => {
                //Create dbObjects from first recordSets result
                var dbObjects = recordSets[0];

                //Filter dbObjects to only include schemas, procedures and functions
                dbObjects = dbObjects.filter((item) => ['schema', 'procedure', 'function'].indexOf(item.type) >= 0);

                //Checking for option to only download selected db objects
                if(typeof options.dbObjects != 'undefined') {
                    //Filter dboObjects to only include dbObjects provided in options.dbObjects
                    dbObjects = dbObjects.filter((item) => options.dbObjects.some((subItem) => item.name == subItem.name));
                }

                //Number of objects
                var total = {
                    procedures: dbObjects.filter((item) => item.type == 'procedure').length,
                    functions: dbObjects.filter((item) => item.type == 'function').length,
                    schemas: dbObjects.filter((item) => item.type == 'schema').length
                };
                total.all = dbObjects.length;

                //Array for holding promises
                var subs = [];

                if(typeof options.callback != 'undefined') {
                    options.callback({
                        info: 'init'
                    });
                }

                //Loop through each dbo object
                dbObjects.forEach((dbObject, index) => {
                    //Add new promise to subs
                    subs.push(() => {
                        return new Promise((resolve, reject) => {
                            //If db object type is schema
                            if(dbObject.type == 'schema') {
                                if(typeof options.callback != 'undefined') { options.callback({
                                    dbObject: dbObject,
                                    info: 'create_ddl',
                                    total: total.all,
                                    index: index + 1
                                }); }

                                //Create new file in schema root
                                fs.writeFile(root[dbObject.type] + '/' + dbObject.name + '.sql', 'CREATE SCHEMA ' + dbObject.name, 'utf8', (error) => {
                                    if(error) { reject(error); return; }

                                    resolve();
                                });
                                //Else if db object type is procedure or function
                            } else if(['procedure', 'function'].indexOf(dbObject.type) >= 0) {
                                //Get DDL for db object
                                this.getDDL(dbObject.name).then((ddl) => {
                                    if(typeof options.callback != 'undefined') { options.callback({
                                        dbObject: dbObject,
                                        info: 'create_ddl',
                                        total: total.all,
                                        index: index + 1
                                    }); }

                                    //Write ddl to file
                                    fs.writeFile(root[dbObject.type] + '/' + dbObject.name + '.sql', ddl, 'utf8', (error) => {
                                        if(error) { reject(error); return; }

                                        resolve();
                                    });
                                }).catch((error) => reject(error));
                            }
                        })
                    });
                });

                //Execute all promises in subs
                Prom.sequence(subs).then((results) => {
                    if (results.some(_.isError)) { //Any error
                        reject(results.filter(_.isError));
                        return;
                    }

                    if(typeof options.callback != 'undefined') { options.callback({
                        info: 'done',
                    }); }

                    resolve();
                });
            }).catch((error) => reject(error));
        });
    },
    getDDL: function(objectReference, options, counter) {
        //Setting options to empty object if not defined
        if(typeof options == 'undefined') { options = {}; }

        //Setting counter to zero if not existing
        if(typeof counter == 'undefined') { counter = 0; }

        return new Promise((resolve, reject) => {
            //The procedure name to use for getDLL command
            var ddlProcedureName = 'GetDDL';
            //Set the query for checking if procedure exists or not
            var query = Str.replace(['%DDL_PROCEDURE_NAME%', '%OBJECT_NAME%'], [ddlProcedureName, objectReference], fs.readFileSync(__dirname + '/resources/sql/get_ddl.sql', 'utf8'));
            this.query(query, options).then((recordSets) => {
                //returnValue to use the first recordSet
                var returnValue = recordSets[0][0];
                /*If returnValue has the field "exists" is equal to "NO"
                 Means no DDL procedure exists and we need to create it*/
                if(typeof returnValue.exists != 'undefined' && returnValue.exists == 'NO') {
                    /*The counter is just for checking that we're
                     not stuck in an infinite loop since the method
                     is calling itself*/
                    if(counter >= 1) {
                        reject(new Error('Unable to create temporary ' + ddlProcedureName + ' procedure'));
                    } else {
                        /*No procedure exists. Lets create it. First by preparing
                         the query. Adding the procedure name*/
                        var query = Str.replace(['%DDL_PROCEDURE_NAME%'], [ddlProcedureName], fs.readFileSync(__dirname + '/resources/sql/create_get_ddl.sql', 'utf8'));
                        this.query(query, options).then(() => {
                            this.getDDL(objectReference, options, ++counter).then((ddl) => resolve(ddl)).catch((error) => reject(error));
                        }).catch((error) => reject(error));
                    }
                } else {
                    /*The procedure exists and it was executed. Lets get the
                     return value (in this case called item)*/
                    resolve(returnValue.item);
                }
            }).catch((error) => reject(error));
        });
    },
    dumpToTempTable: function(query, options) {
        return new Promise((resolve, reject) => {
            if(Obj.getType(options) == undefined) { options = {}; }

            this.query(query, options).then((recordSets) => {
                tableStatements = [];
                recordSets.forEach((recordSet, index) => {
                    if(Obj.getType(options.autoTableName) != undefined && options.autoTableName) {
                        var tableName = '#temp_table' + (index + 1);
                    } else {
                        var tableName = recordSet.query.tableName;
                    }
                    var statement = 'SELECT * INTO ' + tableName + ' FROM (\n';
                    recordSet.forEach((row) => {
                        statement += 'SELECT ';
                        Object.keys(row).forEach((column) => {
                            if(typeof row[column] != 'string') {
                                statement += row[column] + ' ';
                            } else {
                                //Replace any ' with '' for SQL escape
                                statement += '\'' + row[column].replace(/'/g, '\'\'') + '\' ';
                            }
                            statement += ' AS ['+ column + '], ';
                        });
                        statement = Str.substr(statement, 0, -2) + '\nUNION\n';
                    });
                    statement = Str.substr(statement, 0, -6) + ') AS x';

                    tableStatements.push(statement);
                });

                resolve(tableStatements);
            }).catch((error) => reject(error));
        });
    },
    dumpTable: function(table, options) {
        return new Promise((resolve, reject) => {
            if(Obj.getType(options) == undefined) { reject(new Error('Options parameter is missing')); return; }

            if(Obj.getType(options.callback) == undefined) { reject(new Error('Callback is missing from options')); return; }

            var hasRows = false;
            var identityColumn = false;
            var identityColumnIndex = false;

            var query = Str.replace(['%TABLE_NAME%'], [table], fs.readFileSync(__dirname + '/resources/sql/get_identity_column.sql', 'utf8'));
            this.query(query, {
                connection: (Obj.getType(options.connection != undefined) ? options.connection : this.connection)
            }).then((recordSets) => {
                if(recordSets[0].length == 1 && Obj.getType(recordSets[0][0].name) != undefined) {
                    identityColumn = recordSets[0][0].name;
                }

                var insertStatement = '';
                var rowCounter = 0;

                this.query('SELECT * FROM ' + table, {
                    connection: (Obj.getType(options.connection != undefined) ? options.connection : this.connection),
                    stream: true,
                    streamCallback: (type, data) => {
                        if(type == 'recordset') {
                            var columnString = ' (';
                            Object.keys(data.columns).forEach((column, index) => {
                                if((Obj.getType(options.identityInsert) != undefined && options.identityInsert) || column != identityColumn) {
                                    columnString += '[' + column + '], ';
                                } else if (column == identityColumn) {
                                    identityColumnIndex = index;
                                }
                            });

                            columnString = Str.substr(columnString, 0, -2) + ')';

                            //Should the table be truncated first or not
                            if(Obj.getType(options.truncate) != undefined && options.truncate) {
                                options.callback({
                                    statement: 'TRUNCATE TABLE ' + table + '\n',
                                    status: 'progress',
                                    info: 'truncate_table'
                                });
                            }

                            //Should identity columns be fetch or not
                            if(Obj.getType(options.identityInsert) && options.identityInsert && identityColumn !== false) {
                                options.callback({
                                    statement: 'SET IDENTITY_INSERT ' + table + ' ON\n',
                                    status: 'progress',
                                    info: 'identity_insert_on'
                                });
                            }

                            insertStatement = 'INSERT INTO ' + table + columnString;
                            options.callback({
                                statement: insertStatement,
                                status: 'progress',
                                info: 'insert_statement'
                            });

                        } else if(type == 'row') {
                            var rowString = '';
                            if(!hasRows) {
                                rowString += ' VALUES\n';
                                hasRows = true;
                            } else {
                                rowString += ', \n';
                            }
                            rowString += '(';
                            Object.keys(data.row).forEach((field, index) => {
                                if(identityColumnIndex !== index) {
                                    if(typeof data.row[field] != 'string') {
                                        rowString += data.row[field] + ', ';
                                    } else {
                                        //Replace any ' with '' for SQL escape
                                        rowString += '\'' + data.row[field].replace(/'/g, '\'\'') + '\', ';
                                    }
                                }
                            });

                            rowString = '' + Str.substr(rowString, 0, - 2) + ')';
                            options.callback({
                                statement: rowString,
                                status: 'progress',
                                info: 'insert_value'
                            });

                            rowCounter++;

                            if(rowCounter >= 1000) {
                                rowCounter = 0;
                                hasRows = false;

                                options.callback({
                                    statement: '\n' + insertStatement,
                                    status: 'progress',
                                    info: 'insert_statement'
                                });
                            }
                        }
                    }
                }).then(() => {
                    if(Obj.getType(options.identityInsert) && options.identityInsert && identityColumn !== false) {
                        options.callback({
                            statement: '\nSET IDENTITY_INSERT ' + table + ' OFF\n',
                            status: 'progress',
                            info: 'identity_insert_off'
                        });
                    }

                    resolve();
                }).catch((error) => reject(error));
            }).catch((error) => reject(error));
        });
    },
    getComment: function(name, options) {
        return new Promise((resolve, reject) => {
            var query = Str.replace(['%COMMENT_NAME%'], [name], fs.readFileSync(__dirname + '/resources/sql/get_comment.sql', 'utf8'));
            if(Obj.getType(options.export) != undefined && options.export) {
                resolve(query);
                return;
            }

            this.query(query, options).then((recordSets) => {
                var recordSet = recordSets[0];
                if(typeof recordSet[0] != 'undefined' && typeof recordSet[0].value != 'undefined') {
                    resolve(recordSet[0].value);
                } else {
                    resolve();
                }
            }).catch((error) => reject(error));
        });
    },
    setComment: function(name, value, options) {
        //If value is false, empty or undefined call to delete comment instead
        if(!value || value == '' || Obj.getType(value) == undefined) {
            return this.deleteComment(name, options);
        }

        return new Promise((resolve, reject) => {
            var query = Str.replace(['%COMMENT_NAME%', '%COMMENT_VALUE%'], [name, value], fs.readFileSync(__dirname + '/resources/sql/set_comment.sql', 'utf8'));
            if(Obj.getType(options.export) != undefined && options.export) {
                resolve(query);
                return;
            }

            this.query(query, options).then(() => {
                resolve();
            }).catch((error) => reject(error));
        });
    },
    deleteComment: function(name, options) {
        return new Promise((resolve, reject) => {
            var query = Str.replace(['%COMMENT_NAME%'], [name], fs.readFileSync(__dirname + '/resources/sql/delete_comment.sql', 'utf8'));
            if(Obj.getType(options.export) != undefined && options.export) {
                resolve(query);
                return;
            }

            this.query(query, options).then(() => {
                resolve();
            }).catch((error) => reject(error));
        });
    }
}

/**
 * Converts a record set from an associated array to a value array. Example:
 * RecordSet = {
 *  fields:
 *  { field1: 'value1', field2: ['value1', 'value2'] },
 *  { field1: 'value3', field2: ['value4', 'value5'] }
 * ]
 *
 * will result in:
 * RecordSet = {
 *  fields: ['field1', 'field2', 'field2']
 *  rows: [
 *      ['value1', 'value2', 'value3'],
 *      ['value3', 'value4', 'value5']
 *  ]
 * }
 *
 *
 * @param {Array} recordSet
 * @returns {{fields: *, rows: Array}}
 */
SQL.fetchArray = function(recordSet) {
    var fields = [], rows = [];
    recordSet.forEach((row) => {
        var tempRow = [];
        Object.keys(row).forEach((field) => {
            if(Arr.isArray(row[field])) {
                row[field].forEach((subValue) => {
                    if(rows.length == 0) {
                        fields.push(field);
                    }
                    tempRow.push(subValue);
                });
            } else {
                if(rows.length == 0) {
                    fields.push(field);
                }
                tempRow.push(row[field]);
            }
        });

        rows.push(tempRow);
    });

    return { fields: fields, rows: rows };
}

/**
 * Separates each query in a batch seperated by the GO statement. Only supports
 * when GO is on a separate line (case insensitive)
 * @param {String} query
 * @returns {Array}
 */
SQL.queryBatch = function(query) {
    var lines = query.split('\n');
    var queries = [];
    var queryHolder = '';
    for(var a = 0; a < lines.length; a++) {
        if(lines[a].match(/^\s*go\s*$/i) != null) {
            queries.push(queryHolder);
            queryHolder = '';
        } else {
            if(queryHolder == '') {
                queryHolder += lines[a]
            } else {
                queryHolder += '\n' + lines[a];
            }
        }
    }

    if(queryHolder != '') {
        queries.push(queryHolder);
    }

    return queries;
}

/**
 * Converts string to object with name and string as statement property
 * This is mainly for use with the SQL.prototype.query method
 * @param query
 * @returns {Object}
 */
SQL.queryData = function(query) {
    //If query is string
    if(Obj.getType(query) == 'String') {
        //Return object
        return {
            name: 'unknown query',
            statement: query
        }
    }

    //Otherwise the query object as is
    return query;
}

module.exports = SQL;