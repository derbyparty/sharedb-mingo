# livedb-mongo-memory

MongoDB like in memomy storage for [livedb](https://github.com/share/livedb). 
This driver can be used both as a snapshot store and oplog.
 
The driver is written just for fun and it's not for the real use (maybe just for tests). 
It is just the way for me to learn livedb more deeply.

It based on [mingo](https://github.com/kofrasa/mingo) so it supports mongo queries with:
`$gt`, `$gte`, `$lt`, `$lte`, `$ne`, `$nin`, `$in`, `$and`, `$or`, `$nor`, `$not`, 
`$regex`, `$mod`, `$where`, `$all`, `$elemMatch`, `$size`, `$exists`, `$type`,
 `$orderby` mongo specific operations and also `$count`, `$skip`, `$limit` - 
 livedb-mongo specific operations.

At the moment it doesn't support $distinct, $mapReduce and $aggregate operators.

## Usage

```javascript
var livedbmongo = require('livedb-mongo-memory');
var mongo = livedbmongo(options);

var livedb = require('livedb').client(mongo); // Or whatever. See livedb's docs.
```

## MIT License
Copyright (c) 2014 by Artur Zayats

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

