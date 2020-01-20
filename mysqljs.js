const MysqlJs = (req, isDebug = true) => {
  const conns = req.app.locals.settings.pool;
  const debug = isDebug ? console.log : () => {};

  /**
   * Finds rows from a table matching condition. If no limit is
   * provided (or 0) then all rows matching condition are returned.
   *
   * @param {string} table - table name
   * @param {object} conditions - filter conditions
   * @param {int} limit - limit results
   * @returns {array} - array of rows
   */
  const find = (table, conditions = {}, limit = 0) => {
    return new Promise((resolve, reject) => {
      const conditionFields = Object.keys(conditions);
      const whereSql = conditionFields.map(field => '`' + field + '` = ' + 
        conns.escape(conditions[field])).join(' AND ') || '1';
      const limitSql = !limit ? '' : `limit ${parseInt(limit)}`;
      const sql = `select * from ${table} where ${whereSql} ${limitSql}`;
      debug('- find: ', sql);
      conns.query(sql, function (error, results, fields) {
        if (error) reject(error);
        resolve(results);
      });
    });
  }

  /**
   * Finds one row in table matching condition. Same as find 
   * but limit to one. @see find()
   * 
   * @param {string} table - table name
   * @param {object} conditions - filter conditions
   * @returns {object} - row if found or null if nothing found 
   */
  const findOne = (table, conditions) => {
    return new Promise((resolve, reject) => {
      find(table, conditions, 1)
        .then((results) => {
          const row = Array.isArray(results) && results.length > 0 ? results[0] : null;
          resolve(row);
        })
        .catch((err) => reject(err))
    });
  }

  /**
   * Arbitrary query with any number of params, where params is
   * a map of params used in query.
   * 
   * Example query: "select * from users where user_id = $user_id" 
   *  with params object: { user_id: 1234 }
   *
   * @param {string} sql - custom sql query
   * @param {object} params - params used in sql query
   * @return {array} - array of row objects if select
   */
  const query = (sql, params = {}, offset = 0, limit = 0) => {
    return new Promise((resolve, reject) => {
      let sqlWithParams = sql;
      const fields = Object.keys(params);
      if(fields.length > 0) {
        for(const field of fields) {
          sqlWithParams = sqlWithParams.replace('$' + field, conns.escape(params[field]));
        }
      }

      if(limit > 0) {
        sqlWithParams += ` limit ${parseInt(offset)}, ${parseInt(limit)}`;
      }

      debug('- query: ', sqlWithParams);
      conns.query(sqlWithParams, function (error, results, fields) {
        if (error) reject(error);
        resolve(results);
      });
    });
  }

  /**
   * Insert one record into a table with data
   *
   * @param {string} table - table name
   * @param {object} data - data to insert where keys are columns
   */
  const insertOne = (table, data) => {
    if(!data) {
      reject(new Error('invalid insert data'))
    }
    return new Promise((resolve, reject) => {
      const fields = Object.keys(data).map(key => '`' + key +'`').join(', ');
      const values = Object.values(data).map(val => conns.escape(val)).join(', ');
      const sql = `insert ignore into ${table} (${fields}) values (${values})`;
      debug('- insertOne sql:', sql);
      conns.query(sql, function (error, result, fields) {
        if (error) reject(error);
        const affected = result && result.affectedRows ? result.affectedRows : 0;
        resolve(affected);
      });
    });
  }

  /**
   * Update one record using conditions and data.
   *
   * @param {string} table - table name
   * @param {object} conditions - filters where keys are columns
   * @param {object} data - data to update where keys are columns
   * @param {int} limit - limits number to update or updates all if 0
   */
  const updateMany = (table, conditions, data, limit = 0) => {
    return new Promise((resolve, reject) => {
      if(!conditions) {
        reject(new Error('invalid filters data'))
      }
      if(!data) {
        reject(new Error('invalid update data'))
      }
      const conditionFields = Object.keys(conditions);
      const whereSql = conditionFields.map(field => '`' + field + '` = ' 
        + conns.escape(conditions[field])).join(' AND ') || '1';
      const updateSql = Object.keys(data).map(field => '`' + field + '` = ' 
        + conns.escape(data[field])).join(', ');
      const limitSql = !limit ? `limit ${parseInt(limit)}` : '';
      const sql = `update ${table} set ${updateSql} where ${whereSql} ${limitSql}`;
      debug('- update sql:', sql);
      conns.query(sql, function (error, result, fields) {
        if (error) reject(error);
        const changed = result && result.changedRows ? result.changedRows : 0;
        resolve(changed);
      });
    });
  }

  /**
   * Same as updateMany but only updates one record
   * @see updateMany
   */
  const updateOne = (table, conditions, data) => {
    return updateMany(table, conditions, data, 1);
  }

  /**
   * Delete one record from a table with conditions
   *
   * @param {string} table - table name
   * @param {object} conditions - conditions to check where keys are columns
   * @param {int} limit - limits number to update or updates all if 0
   */
  const deleteMany = (table, conditions, limit = 0) => {
    return new Promise((resolve, reject) => {
      if(!conditions) {
        reject(new Error('invalid filters data'))
      }
      const conditionFields = Object.keys(conditions);
      const whereSql = conditionFields.map(field => '`' + field + '` = ' + conns.escape(conditions[field])).join(' AND ');
      const limitSql = !limit ? `limit ${parseInt(limit)}` : '';
      const sql = `delete from ${table} where ${whereSql} ${limitSql}`;
      debug('- delete sql:', sql);
      conns.query(sql, function (error, result, fields) {
        if (error) reject(error);
        const affected = result && result.affectedRows ? result.affectedRows : 0;
        resolve(affected);
      });
    });
  }

  /**
   * Same as deleteMany but deletes one record only
   * @see deleteMany
   */
  const deleteOne = (table, conditions) => {
    return deleteMany(table, conditions, 1);
  }


  return {
    find,
    findOne,
    query,
    deleteMany,
    deleteOne,
    insertOne,
    updateMany,
    updateOne
  };
};

module.exports = MysqlJs