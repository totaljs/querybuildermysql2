# Total.js QueryBuilder: MySQL

A simple QueryBuilder integrator for MySQL database.

- [Documentation](https://docs.totaljs.com/total4/)
- `$ npm install querybuildermysql2`

__Initialization__:

```js
// require('querybuildermysql2').init(name, connectionstring, pooling, [errorhandling]);
// name {String} a name of DB (default: "default")
// connectionstring {String} a connection to the MySQL
// pooling {Number} max. clients (default: "0" (disabled))
// errorhandling {Function(err, cmd)}

require('querybuildermysql2').init('default', CONF.database);
// require('querybuildermysql').init('default', CONF.database, 10);
```

__Usage__:

```js
DB().find('tbl_user').where('id', 1234).callback(console.log);
// DB('default').find('tbl_user').where('id', 1234).callback(console.log);
```

## Autoquery for JSON columns

`QueryBuilder.autoqueryjson($, column, schema, [defsort], [maxlimit])` works like `autoquery()` but the properties are read from a JSON column. It supports the filtering, sorting, fields and pagination of `autoquery()`.

```js
// $ {Controller|Object} uses $.query (or the object itself)
// column {String} a name of the JSON column
// schema {String} properties and types, e.g. 'id,name,age:Number'
// defsort {String} default sorting, e.g. 'age_desc' (optional)
// maxlimit {Number} max. items per page, it enables the pagination (optional)

DATA.list('tbl_user').autoqueryjson($, 'data', 'id,name,email,age:Number,ok:Boolean,dtcreated:Date,tags:[string]', 'dtcreated_desc', 100).callback($.callback);
```

- Query string: `?age=>18&name=john&tags=a,b&sort=age_desc&fields=name,age&page=2&limit=20`
- Filters use the same rules as `autoquery()` (`a - b` range, `a,b` list, `>` and `<`, `=` exact, `today`...)
- Only properties declared in the `schema` can be used in fields, filters and sorting
- A property without a type is `String`
- The names of the column and properties accept only `a-z`, `0-9` and `_`
- Requires MySQL 5.7+ (`JSON_EXTRACT`, `JSON_CONTAINS`)
- `Boolean` is returned as `1` or `0`
- The time zone in `Date` values is ignored (the value is used as it is written)

__Types and values__:

| Type | Missing property | Value in a wrong format |
|------|------------------|-------------------------|
| `String` | `null` | the value as text |
| `Number` | `null` | `0` |
| `Boolean` | `null` | `false` (`true`, `1`, `on`, `yes` are `true`) |
| `Date` | `null` | `null` |
| `[string]` | `null` | `[]` |

A value in a wrong format never fails the query (e.g. `"age": "abc"` is `0`), the same way as in `autoquery()`. This applies to filtering, sorting and to the returned values.

The QueryBuilder doesn't generate SQL for JSON columns. It uses a neutral name `json:column:property:type` and this adapter translates it into `JSON_EXTRACT(...)` expressions.

