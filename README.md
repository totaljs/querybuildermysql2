# Total.js QueryBuilder: MySQL

A simple QueryBuilder integrator for MySQL database.

- [Documentation](https://docs.totaljs.com/total4/)

__Initialization__:

```js
// require('querybuildermysql').init(name, connectionstring, pooling, [errorhandling]);
// name {String} a name of DB (default: "default")
// connectionstring {String} a connection to the MySQL
// pooling {Number} max. clients (default: "0" (disabled))
// errorhandling {Function(err, cmd)}

require('querybuildermysql').init('default', CONF.database);
// require('querybuildermysql').init('default', CONF.database, 10);
```

__Usage__:

```js
DB().find('tbl_user').where('id', 1234).callback(console.log);
// DB('default').find('tbl_user').where('id', 1234).callback(console.log);
```