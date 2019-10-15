const mysql = require('mysql');
const errors = require('./mysql.errors');
const env = process.env;
let dbConnection;

/**
 * this method get the connection from the connection pool
 * @param {function} callback 
 */
let getConnection = function (callback) {
    let connect = mysql.createPool({
        connectionLimit: env.MYSQL_POOL_SIZE,
        host: env.MYSQL_HOST,
        user: env.MYSQL_USERNAME,
        password: env.MYSQL_PASSWORD,
        database: env.MYSQL_DATABASE,
        multipleStatements: true,
    });
    connect.getConnection((err, connection) => {
        if (err) {
            console.log(err);
            callback(true, err);
        } else {
            console.log('connected');
            dbConnection = connection;
            callback(false, dbConnection);
            connection.release();
        }
    })
}
/**
 * this method get the instance of connection
 * @param {function} callback 
 */
let getInstance = function (callback) {
    if (!dbConnection) {
        console.log('db connection false');
        getConnection(callback);
    } else {
        console.log('db connection true');
        callback(false, dbConnection);
    }
};
/**
 * we should check our database has the table, it will do with this method
 * @param {object} model 
 * @param {string} table 
 * @param {function} callback 
 */
let validateTable = function (model, table, callback) {
    getInstance(function (error, connection) {
        if (error) {
            callback(true, 'error');
        } else {
            connection.query('show tables like  "' + table + '"', (error, dbResponse) => {
                if (error) {
                    dbConnection = false;
                    //callback(true, 'error');
                } else {
                    if (dbResponse.length === 0) {
                        connection.query('create table ' + table + ' (' + Object.keys(model).map(k => k + ' ' + model[k] + '') + ')', (error, dbResponse) => {
                            if (error) {
                                dbConnection = false;
                                //callback(true, 'error');
                            } else {
                                callback(error, connection);
                            }
                        })
                    } else {
                        callback(false, connection);
                    }
                }
            })
        }
    })
}

module.exports = function (table, model) {
    /**
     * this method will insert data to my sql table
     * @param {function} callback
     * @param {object} data
     * @returns void
     */
    this.save = function (data, callback) {
        if (data === null || typeof data !== 'object') {
            callback(true, errors.SAVING_ERROR);
        } else {
            validateTable(model, table, function (error, connection) {
                if (error) {
                    callback(true, errors.VALIDATE_TABLE_ERROR);
                } else {
                    connection.query('insert into ' + table + ' SET ?', data, (error, dbResponse) => {
                        if (error) {
                            console.error(error);
                            callback(true, errors.SAVING_ERROR);
                        } else {
                            callback(false, dbResponse);
                        }
                    })
                }
            })
        }
    };
    /**
     * we can select data with this method
     * this method will provide query like this 'select * from mytable where id=1'
     * @param {object} data
     * @param {function} callback
     * @returns void
     */
    this.getOne = function (data, callback) {
        if (Object.keys(data).length > 1) {
            callback(true, errors.SELECT_QUERY_PARAMS_LENGTH_IS_NOT_VALIED);
        } else {
            validateTable(model, table, function (error, connection) {
                if (error) {
                    callback(true, connection);
                } else {
                    connection.query("select * from " + table + " where ?", data, (err, results) => {
                        if (err) {
                            console.error(err + data.length);
                            callback(true, errors.MYSQL_SYNTAX_ERROR);
                        } else {
                            if (results.length === 0) {
                                callback(true, errors.RECORD_NOT_FOUND);
                            } else {
                                callback(false, results[0]);
                            }

                        }
                    })
                }
            })
        }
    };
    /**
     * this method will update a record this method will provide query like update mytable set username= jhon doe where id=1
     * @param {object} data
     * @param {function} callback
     * @returns void
     */
    this.update = function (data, callback) {
        validateTable(model, table, function (validationError, connection) {
            if (validationError) {
                callback(true, connection);
            } else {
                connection.query("update " + table + " SET ? ", data, (updateError, responseFromDatabase) => {
                    if (updateError) {
                        console.error(updateError);
                        callback(true, errors.MYSQL_SYNTAX_ERROR);
                    } else {
                        callback(false, responseFromDatabase);
                    }
                })
            }
        })
    };
    /**
     * this metho get record values by a condition this method will provide a query like select * from mytable where id<1
     * @param {string} greaterThanWhat
     * @param {string} condition
     * @param {object} data
     * @param {function} callback
     * @returns void
     */
    this.getByCondition = function (greaterThanWhat, condition, data, callback) {
        validateTable(model, table, function (validationError, connection) {
            if (validationError) {
                callback(true, connection);
            } else {
                connection.query(`select * from ${table} where ${greaterThanWhat}${condition} ?`, data, (databaseError, dbResponse) => {
                    if (databaseError) {
                        console.error(databaseError);
                        callback(true, errors.MYSQL_SYNTAX_ERROR);
                    } else {
                        callback(false, dbResponse);
                    }
                })
            }
        });
    };

    /**
     * this method will return all results of a table with pageble
     * @param {string} coloumnName
     * @param {string} order
     * @param {number} limit
     * @param {number} pageNumber
     * @param {function} callback
     * @returns void
     */
    this.getAllPagable = function (order, coloumnName, limit, pageNumber, callback) {
        validateTable(model, table, function (validationError, connection) {
            if (validationError) {
                callback(true, connection);
            } else {
                let offSet = (limit * pageNumber) - limit;
                //let offSet= 0;
                connection.query(`select * from ${table} order by ${coloumnName} ${order} limit ${offSet},${limit}; select count(*) from ${table}`, (databaseError, dbResponse) => {
                    if (databaseError) {
                        console.error(databaseError);
                        callback(true, errors.VALIDATE_TABLE_ERROR);
                    } else {
                        let results = {
                            data: dbResponse[0],
                            counts: dbResponse[1][0],
                            pages: Math.round(dbResponse[1][0]['count(*)'] / limit)
                        }
                        callback(false, results);
                    }
                })
            }
        })
    };
    /**
     * this method will delete a record from table this method will provide a query like delete from mytable where id='1'
     * to run above query the data object should like this data={id : 1}
     * data object should contain only one value
     * @param {object} data
     * @param {function} callback
     * @returns void
     */
    this.deleteBy = (data, callback) => {
        validateTable(model, table, (validationError, connection) => {
            if (validationError) {
                callback(true, connection);
            } else {
                connection.query(`delete from ${table} where ?`,data, (databaseError, dbResponse) => {
                    if (databaseError) {
                        console.error(databaseError);
                        callback(true, errors.MYSQL_SYNTAX_ERROR);
                    } else {
                        callback(false, dbResponse);
                    }
                })
            }
        });
    }
}