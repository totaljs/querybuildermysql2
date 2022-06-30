// Total.js Module: MySQL integrator

const CANSTATS = global.F ? (global.F.stats && global.F.stats.performance && global.F.stats.performance.dbrm != null) : false;
const MySQL = require('mysql2');
const POOLS = {};
const LOGGER = '-- MySQL -->';
const REG_LANGUAGE = /[a-z0-9]+§/gi;

function exec(client, filter, callback, done, errorhandling) {

	var cmd;

	if (filter.exec === 'list') {

		cmd = makesql(filter);

		if (filter.debug)
			console.log(LOGGER, cmd.query, cmd.params);

		client.query(cmd.query, cmd.params, function(err, response) {
			if (err) {
				done();
				errorhandling && errorhandling(err, cmd);
				callback(err);
			} else {

				cmd = makesql(filter, 'count');

				if (filter.debug)
					console.log(LOGGER, cmd.query, cmd.params);

				client.query(cmd.query, cmd.params, function(err, counter) {
					done();
					err && errorhandling && errorhandling(err, cmd);
					callback(err, err ? null : { items: response, count: +counter[0].count });
				});
			}
		});
		return;
	}

	cmd = makesql(filter);

	if (filter.debug)
		console.log(LOGGER, cmd.query, cmd.params);

	client.query(cmd.query, cmd.params, function(err, response) {

		done();

		if (err) {
			errorhandling && errorhandling(err, cmd);
			callback(err);
			return;
		}

		switch (filter.exec) {
			case 'insert':
				callback(null, filter.primarykey ? response.length && response[0][filter.primarykey] : 1);
				break;
			case 'update':
				callback(null, response.affectedRows || 0);
				break;
			case 'remove':
				callback(null, response.affectedRows || 0);
				break;
			case 'check':
				callback(null, response ? response > 0 : false);
				break;
			case 'count':
				callback(null, response[0] ? response[0].count : null);
				break;
			case 'scalar':
				if (filter.scalar.type === 'group')
					callback(null, response);
				else
					callback(null, response[0] ? response[0].value : null);
				break;
			default:
				callback(err, response);
				break;
		}
	});
}

function db_where(where, opt, filter, operator) {

	var tmp;

	for (var item of filter) {

		if (opt.language != null && item.name && item.name[item.name.length - 1] === '§')
			item.name = item.name.substring(0, item.name.length - 1) + opt.language;

		switch (item.type) {
			case 'or':
				tmp = [];
				db_where(tmp, opt, item.value, 'OR');
				where.length && where.push(operator);
				where.push('(' + tmp.join(' ') + ')');
				break;
			case 'in':
			case 'notin':
				where.length && where.push(operator);
				tmp = [];
				if (item.value instanceof Array) {
					for (var val of item.value) {
						if (val != null)
							tmp.push(MYSQL_ESCAPE(val));
					}
				} else if (item.value != null)
					tmp = [MYSQL_ESCAPE(item.value)];
				if (!tmp.length)
					tmp.push('null');
				where.push(item.name + (item.type === 'in' ? ' IN ' : ' NOT IN ') + '(' + tmp.join(',') + ')');
				break;
			case 'query':
				where.length && where.push(operator);
				where.push('(' + item.value + ')');
				break;
			case 'where':
				where.length && where.push(operator);
				if (item.value == null)
					where.push(item.name + (item.comparer === '=' ? ' IS NULL' : ' IS NOT NULL'));
				else
					where.push(item.name + item.comparer + MYSQL_ESCAPE(item.value));
				break;
			case 'contains':
				where.length && where.push(operator);
				where.push('LENGTH(' + item.name +')>0');
				break;
			case 'search':
				where.length && where.push(operator);
				tmp = item.value.toLowerCase().replace(/%/g, '');
				if (item.operator === 'beg')
					where.push(item.name + ' LIKE ' + MYSQL_ESCAPE('%' + tmp));
				else if (item.operator === 'end')
					where.push(item.name + ' LIKE ' + MYSQL_ESCAPE(tmp + '%'));
				else
					where.push(item.name + ' LIKE ' + MYSQL_ESCAPE('%' + tmp + '%'));
				break;
			case 'month':
			case 'year':
			case 'day':
			case 'hour':
			case 'minute':
				where.length && where.push(operator);
				where.push(item.type + '(`' + item.name + '`)' + item.comparer + MYSQL_ESCAPE(item.value));
				break;
			case 'empty':
				where.length && where.push(operator);
				where.push('(' + item.name + ' IS NULL OR LENGTH(' + item.name + ')=0)');
				break;
			case 'between':
				where.length && where.push(operator);
				where.push('(' + item.name + ' BETWEEN ' + MYSQL_ESCAPE(item.a) + ' AND ' + MYSQL_ESCAPE(item.b) + ')');
				break;
		}
	}
}

function db_insertupdate(filter, insert) {

	var query = [];
	var fields = insert ? [] : null;
	var params = [];

	for (var key in filter.payload) {
		var val = filter.payload[key];
		var c = key[0];
		switch (c) {
			case '-':
			case '+':
			case '*':
			case '/':
				key = key.substring(1);
				params.push(val ? val : 0);
				if (insert) {
					fields.push('`' + key + '`');
					query.push('?');
				} else
					query.push('`' + key + '`=COALESCE(' + key + ',0)' + c + '?');
				break;
			case '>':
			case '<':
				key = key.substring(1);
				params.push(val ? val : 0);
				if (insert) {
					fields.push('`' + key + '`');
					query.push('?');
				} else
					query.push('`' + key + '`=' + (c === '>' ? 'GREATEST' : 'LEAST') + '(' + key + ',?)');
				break;
			case '!':
				// toggle
				key = key.substring(1);
				if (insert) {
					fields.push('`' + key + '`');
					query.push('0');
				} else
					query.push('`' + key + '`=NOT `' + key + '`');
				break;
			case '=':
			case '#':
				// raw
				key = key.substring(1);
				if (insert) {
					if (c === '=') {
						fields.push('`' + key + '`');
						query.push(val);
					}
				} else
					query.push('`' + key + '`=' + val);
				break;
			default:
				params.push(val);
				if (insert) {
					fields.push('`' + key + '`');
					query.push('?');
				} else
					query.push('`' + key + '`=?');
				break;
		}
	}

	return { fields, query, params };
}

function replacelanguage(fields, language, noas) {
	return fields.replace(REG_LANGUAGE, function(val) {
		val = val.substring(0, val.length - 1);
		return val + (noas ? language : (language ? (language + ' as ' + val) : ''));
	});
}

function makesql(opt, exec) {

	var query = '';
	var where = [];
	var model = {};
	var isread = false;
	var params;
	var index;
	var tmp;

	if (!exec)
		exec = opt.exec;

	db_where(where, opt, opt.filter, 'AND');

	if (opt.language != null && opt.fields)
		opt.fields = replacelanguage(opt.fields.join(','), opt.language);

	switch (exec) {
		case 'find':
		case 'read':
			query = 'SELECT ' + (opt.fields || '*') + ' FROM ' + opt.table + (where.length ? (' WHERE ' + where.join(' ')) : '');
			isread = true;
			break;
		case 'list':
			query = 'SELECT ' + (opt.fields || '*') + ' FROM ' + opt.table + (where.length ? (' WHERE ' + where.join(' ')) : '');
			isread = true;
			break;
		case 'count':
			opt.first = true;
			query = 'SELECT COUNT(1) as count FROM ' + opt.table + (where.length ? (' WHERE ' + where.join(' ')) : '');
			isread = true;
			break;
		case 'insert':
			tmp = db_insertupdate(opt, true);
			query = 'INSERT INTO ' + opt.table + ' (' + tmp.fields.join(',') + ') VALUES(' + tmp.query.join(',') + ')';
			params = tmp.params;
			break;
		case 'remove':
			query = 'DELETE FROM ' + opt.table + (where.length ? (' WHERE ' + where.join(' ')) : '');
			break;
		case 'update':
			tmp = db_insertupdate(opt);
			query = 'UPDATE ' + opt.table + ' SET ' + tmp.query.join(',') + (where.length ? (' WHERE ' + where.join(' ')) : '');
			params = tmp.params;
			break;
		case 'check':
			query = 'SELECT 1 as count FROM ' + opt.table + (where.length ? (' WHERE ' + where.join(' ')) : '');
			isread = true;
			break;
		case 'drop':
			query = 'DROP TABLE ' + opt.table;
			break;
		case 'truncate':
			query = 'TRUNCATE TABLE ' + opt.table + ' RESTART IDENTITY';
			break;
		case 'command':
			break;
		case 'scalar':
			switch (opt.scalar.type) {
				case 'avg':
				case 'min':
				case 'sum':
				case 'max':
				case 'count':
					opt.first = true;
					var val = opt.scalar.key === '*' ? 1 : opt.scalar.key;
					query = 'SELECT ' + opt.scalar.type.toUpperCase() + (opt.scalar.type !== 'count' ? ('(' + val + ')') : '(1)') + ' as value FROM ' + opt.table + (where.length ? (' WHERE ' + where.join(' ')) : '');
					break;
				case 'group':
					query = 'SELECT ' + opt.scalar.key + ', ' + (opt.scalar.key2 ? ('SUM(' + opt.scalar.key2 + ')') : 'COUNT(1)') + ' as value FROM ' + opt.table + (where.length ? (' WHERE ' + where.join(' ')) : '') + ' GROUP BY ' + opt.scalar.key;
					break;
			}
			isread = true;
			break;
		case 'query':
			query = opt.query + (where.length ? (' WHERE ' + where.join(' ')) : '');
			isread = true;
			break;
	}

	if (exec === 'find' || exec === 'read' || exec === 'list' || exec === 'query' || exec === 'check') {

		if (opt.sort) {

			tmp = '';

			for (var i = 0; i < opt.sort.length; i++) {
				var item = opt.sort[i];
				index = item.lastIndexOf('_');
				tmp += (i ? ', ' : ' ') + item.substring(0, index) + ' ' + (item.substring(index + 1) === 'desc' ? 'DESC' : 'ASC');
			}

			if (opt.language != null)
				tmp = replacelanguage(tmp, opt.language, true);

			query += ' ORDER BY' + tmp;
		}

		if (opt.take && opt.skip)
			query += ' LIMIT ' + opt.take + ' OFFSET ' + opt.skip;
		else if (opt.take)
			query += ' LIMIT ' + opt.take;
		else if (opt.skip)
			query += ' OFFSET ' + opt.skip;
	}

	model.query = query;
	model.params = params;

	if (CANSTATS) {
		if (isread)
			F.stats.performance.dbrm++;
		else
			F.stats.performance.dbwm++;
	}

	return model;
}

function MYSQL_ESCAPE(value) {

	if (value == null)
		return 'null';

	var type = typeof(value);

	if (type === 'function') {
		value = value();
		if (value == null)
			return 'null';
		type = typeof(value);
	}

	if (type === 'boolean')
		return value === true ? 'true' : 'false';

	if (type === 'number')
		return value + '';

	if (type === 'string')
		return MySQL.escape(value);

	if (value instanceof Array)
		return MySQL.escape(value.join(','));

	if (value instanceof Date)
		return dateToString(value);

	return MySQL.escape(value.toString());
}

function dateToString(dt) {

	var arr = [];

	arr.push(dt.getFullYear().toString());
	arr.push((dt.getMonth() + 1).toString());
	arr.push(dt.getDate().toString());
	arr.push(dt.getHours().toString());
	arr.push(dt.getMinutes().toString());
	arr.push(dt.getSeconds().toString());

	for (var i = 1; i < arr.length; i++) {
		if (arr[i].length === 1)
			arr[i] = '0' + arr[i];
	}

	return arr[0] + '-' + arr[1] + '-' + arr[2] + ' ' + arr[3] + ':' + arr[4] + ':' + arr[5];
}

global.MYSQL_ESCAPE = MYSQL_ESCAPE;

exports.init = function(name, connstring, pooling, errorhandling) {

	if (!name)
		name = 'default';

	if (pooling)
		pooling = +pooling;

	if (!connstring) {

		if (POOLS[name]) {
			var conn = POOLS[name];
			conn.end && conn.end();
			conn.destroy && conn.destroy();
			delete POOLS[name];
		}

		// Removes instance
		NEWDB(name, null);
		return;
	}

	connstring = require('url').parse(connstring);
	var opt = {};
	var auth = (connstring.auth || '').split(':');
	opt.host = connstring.hostname;
	opt.user = auth[0];
	opt.password = auth[1];
	opt.database = connstring.pathname.substring(1);
	opt.connectionLimit = pooling;

	NEWDB(name, function(filter, callback) {
		if (pooling) {
			var pool = POOLS[name] || (POOLS[name] = MySQL.createPool(opt));
			pool.getConnection(function(err, client) {
				if (err)
					callback(err);
				else
					exec(client, filter, callback, () => pool.releaseConnection(client), errorhandling);
			});
		} else {
			var conn = MySQL.createConnection(opt);
			exec(conn, filter, callback, () => conn.destroy(), errorhandling);
		}
	});
};