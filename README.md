# sharedb-mingo

MongoDB like in memomy storage for [sharedb](https://github.com/share/sharedb). 
This driver can be used both as a snapshot, store and oplog.
 
The driver is written just for fun and it's not for the real use (maybe just for tests). 
It is just the way for me to learn sharedb more deeply.

It based on [mingo](https://github.com/kofrasa/mingo) so it supports mongo queries with:
`$gt`, `$gte`, `$lt`, `$lte`, `$ne`, `$nin`, `$in`, `$and`, `$or`, `$nor`, `$not`, 
`$regex`, `$mod`, `$where`, `$all`, `$elemMatch`, `$size`, `$exists`, `$type`,
 `$orderby` mongo specific operations and also `$count`, `$skip`, `$limit` - 
 livedb-mongo specific operations.

At the moment it doesn't support $distinct, $mapReduce operators.

## Usage

```javascript
var sharedbmingo = require('sharedb-mingo');
var mingo = sharedbmingo(options);

var sharedb = require('sharedb').client(mingo); // Or whatever. See sharedb's docs.
```

## MIT License 2016 by Artur Zayats