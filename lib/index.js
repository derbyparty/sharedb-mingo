var Mingo = require('mingo');
var clone = require('clone');

var metaOperators = {
  $comment: true
  , $explain: true
  , $hint: true
  , $maxScan: true
  , $max: true
  , $min: true
  , $orderby: true
  , $returnKey: true
  , $showDiskLoc: true
  , $snapshot: true
  , $count: true
};

var cursorOperators = {
  $limit: 'limit'
  , $skip: 'skip'
  , $orderby: 'sort'
};

// Agenda
// cName - collection name
// docName - id of the specific document

// There are 3 different APIs a database can expose. A database adaptor does
// not need to implement all three APIs. You can pick and choose at will.
//
// The three database APIs are:
//
// - Snapshot API, which is used to store actual document data
// - Query API, which livedb wraps for live query capabilities.
// - Operation log for storing all the operations people have submitted. This
//   is used if a user makes changes while offline and then reconnects. Its
//   also really useful for auditing user actions.
//
// All databases should implement the close() method regardless of which APIs
// they expose.

function LiveDbMongoMemory(options) {
  if (!(this instanceof LiveDbMongoMemory)) return new LiveDbMongoMemory();

  // Map from collection name -> doc name -> snapshot ({v:, type:, data:})
  this.collections = {};

  // Map from collection name -> doc name -> list of operations. Operations
  // don't store their version - instead their version is simply the index in
  // the list.
  this.ops = {};

  this.allowJavaScriptQuery = options ? (options.allowJavaScriptQuery || false) : false;
  this.closed = false;
};

module.exports = LiveDbMongoMemory;

LiveDbMongoMemory.prototype.close = function(callback) {
  if (this.closed) return callback('db already closed');
  this.closed = true;
};


// Snapshot database API

// Get the named document from the database. The callback is called with (err,
// data). data may be null if the docuemnt has never been created in the
// database.
LiveDbMongoMemory.prototype.getSnapshot = function(cName, docName, callback) {
  this.getSnapshotProjected(cName, docName, null, callback);
};

LiveDbMongoMemory.prototype.getSnapshotProjected = function(cName, docName, fields, callback) {

  var obj = this._getDirtySnapshot(cName, docName);

  if (!fields || !obj) return process.nextTick(function() {
    callback(null, castToSnapshot(obj));
  });

  var projection = projectionFromFields(fields);

  obj = cropDocument(obj, projection);

  process.nextTick(function() {
    callback(null, castToSnapshot(obj));
  });
};

LiveDbMongoMemory.prototype._getDirtySnapshot = function (cName, docName){
  var c = this.collections[cName];

  if (!c) return null;

  var query = new Mingo.Query({_id: docName});
  var cursor = query.find(c);

  var obj = null;

  if (cursor.count() === 0) return null;

  return cursor.first();
};


// This function is optional.
//
// - It makes it faster for dedicated indexes (like SOLR) to get a whole bunch
// of complete documents to service queries.
// - It can also improve reconnection time
//
// Its included here for demonstration purposes and so we can test our tests.
//
// requests is an object mapping collection name -> list of doc names for
// documents that need to be fetched.
//
// The callback is called with (err, results) where results is a map from
// collection name -> {docName:data for data that exists in the collection}
//
// Documents that have never been touched can be ommitted from the results.
// Documents that have been created then later deleted must exist in the result
// set, though only the version field needs to be returned.
//
// bulkFetch replaces getBulkSnapshots in livedb 0.2.
LiveDbMongoMemory.prototype.bulkGetSnapshot = function(requests, callback) {
  var results = {};

  for (var cName in requests) {
    var cResult = results[cName] = {};

    var c = this.collections[cName];
    if (!c) continue;

    var docNames = requests[cName];
    for (var i = 0; i < docNames.length; i++) {
      var obj = this._getDirtySnapshot(cName, docNames[i]);

      var snapshot = castToSnapshot(obj);
      if (snapshot) cResult[docNames[i]] = {
        type: snapshot.type,
        v: snapshot.v,
        m: clone(snapshot.m),
        data: clone(snapshot.data)
      };
    }
  }

  process.nextTick(function() {
    callback(null, results);
  });
};

LiveDbMongoMemory.prototype.writeSnapshot = function(cName, docName, snapshot, callback) {
  var c = this.collections[cName] = this.collections[cName] || [];

  // if there is an element with such docName
  for(var i=0; i< c.length; i++){
    var item = c[i];

    if (item._id === docName) {
      c.splice(i, 1, castToDoc(docName, snapshot));
      return process.nextTick(function() { callback()});
    }
  }

  // if not
  c.push(castToDoc(docName, snapshot));


  process.nextTick(function() {
    callback();
  });
};

// ********* Oplog API

// Internal function.
LiveDbMongoMemory.prototype._getOpLog = function(cName, docName) {
  var c = this.ops[cName];
  if (!c) c = this.ops[cName] = {};

  var ops = c[docName]
  if (!ops) ops = c[docName] = [];

  return ops;
};

// This is used to store an operation.
//
// Its possible writeOp will be called multiple times with the same operation
// (at the same version). In this case, the function can safely do nothing (or
// overwrite the existing identical data). It MUST NOT change the version number.
//
// Its guaranteed that writeOp calls will be in order - that is, the database
// will never be asked to store operation 10 before it has received operation
// 9. It may receive operation 9 on a different server.
//
// opData looks like:
// {v:version, op:... OR create:{optional data:..., type:...} OR del:true, [src:string], [seq:number], [meta:{...}]}
//
// callback should be called as callback(error)
LiveDbMongoMemory.prototype.writeOp = function(cName, docName, opData, callback) {
  var opLog = this._getOpLog(cName, docName);

  // This should never actually happen unless there's bugs in livedb. (Or you
  // try to use this memory implementation with multiple frontend servers)
  if (opLog.length < opData.v - 1)
    return process.nextTick(function() {
      callback('Internal consistancy error - database missing parent version');
    });

  opLog[opData.v] = opData;
  process.nextTick(function() {
    callback();
  });
};

// Get the current version of the document, which is one more than the version
// number of the last operation the database stores.
//
// callback should be called as callback(error, version)
LiveDbMongoMemory.prototype.getVersion = function(cName, docName, callback) {
  var opLog = this._getOpLog(cName, docName);
  process.nextTick(function() {
    callback(null, opLog.length);
  });
};

// Get operations between [start, end) noninclusively. (Ie, the range should
// contain start but not end).
//
// If end is null, this function should return all operations from start onwards.
//
// The operations that getOps returns don't need to have a version: field.
// The version will be inferred from the parameters if it is missing.
//
// Callback should be called as callback(error, [list of ops]);
LiveDbMongoMemory.prototype.getOps = function(cName, docName, start, end, callback) {
  var opLog = this._getOpLog(cName, docName);

  if (end == null)
    end = opLog.length;

  process.nextTick(function() {
    callback(null, opLog.slice(start, end));
  });
};


// ********** Query support API.
// This is optional. It allows you to run queries against the data.

// The memory database has a really simple (probably too simple) query
// mechanism to get all documents in the collection. The query is just the
// collection name.

// Run the query itself. The query is ignored - we just return all documents in
// the specified index (=collection).

LiveDbMongoMemory.prototype._query = function(mongo, cName, query, fields, callback) {
  // For count queries, don't run the find() at all. We also ignore the projection, since its not
  // relevant.
  if (query.$count) {
    delete query.$count;

    var c = this.collections[cName];

    if (!c) return process.nextTick(function() {
      callback(null, {results:[], extra:0});
    });

    var mingoQuery = new Mingo.Query(query.$query);
    var cursor = mingoQuery.find(c, projection);

    process.nextTick(function() {
      callback(null, {results: [], extra: cursor.count()});
    });

  } else {
    var cursorMethods = extractCursorMethods(query);

    // Weirdly, if the requested projection is empty, we send everything.
    var projection = fields ? projectionFromFields(fields) : {};

    var c = this.collections[cName];

    if (!c) return process.nextTick(function() {
      callback(null, []);
    });

    var mingoQuery = new Mingo.Query(query.$query);
    var cursor = mingoQuery.find(c, projection);

    for (var i = 0; i < cursorMethods.length; i++) {
      var item = cursorMethods[i];
      var method = item[0];
      var arg = item[1];
      cursor[method](arg);
    }

    var results = cursor.all();

    results = results && results.map(castToSnapshot);
    process.nextTick(function() {
      callback(null, results);
    });
  }
};


LiveDbMongoMemory.prototype.query = function(livedb, cName, inputQuery, opts, callback) {
  this.queryProjected(livedb, cName, null, inputQuery, opts, callback);
};

LiveDbMongoMemory.prototype.queryProjected = function(livedb, cName, fields, inputQuery, opts, callback) {
  var err;
  // TODO var err; if (err = this._check(cName)) return callback(err);
  var query = normalizeQuery(inputQuery);

  if (err = this._checkQuery(query)) return process.nextTick(function() {
    callback(err);
  });

  this._query(this.mongo, cName, query, fields, callback);
};


// Queries can avoid a lot of CPU load by querying individual documents instead
// of the whole collection.
LiveDbMongoMemory.prototype.queryNeedsPollMode = function(index, query) {
  return query.hasOwnProperty('$orderby') ||
      query.hasOwnProperty('$limit') ||
      query.hasOwnProperty('$skip') ||
      query.hasOwnProperty('$count');
};



LiveDbMongoMemory.prototype.queryDoc = function(livedb, index, cName, docName, inputQuery, callback) {
  this.queryDocProjected(livedb, index, cName, docName, null, inputQuery, callback);
};

LiveDbMongoMemory.prototype.queryDocProjected = function(livedb, index, cName, docName, fields, inputQuery, callback) {
  var err;
  // TODO var err; if (err = this._check(cName)) return callback(err);
  var query = normalizeQuery(inputQuery);

  if (err = this._checkQuery(query)) return process.nextTick(function() {
    callback(err);
  });

  // Run the query against a particular mongo document by adding an _id filter
  var queryId = query.$query._id;
  if (queryId) {
    delete query.$query._id;
    query.$query.$and = [{_id: docName}, {_id: queryId}];
  } else {
    query.$query._id = docName;
  }

  var projection = fields ? projectionFromFields(fields) : {};

  function cb(err, doc) {
    process.nextTick(function() {
      callback(err, castToSnapshot(doc));
    });
  }

  var c = this.collections[cName];

  if (!c) return process.nextTick(function() {
    callback(null, null);
  });

  var mingoQuery = new Mingo.Query(query.$query);
  var cursor = mingoQuery.find(c, projection);

  var result = cursor.first();

  process.nextTick(function() {
    callback(null, castToSnapshot(result));
  });

};

// Return error string on error. Query should already be normalized with
// normalizeQuery below.
LiveDbMongoMemory.prototype._checkQuery = function(query) {
  if (!this.allowJavaScriptQuery) {
    if (query.$query.$where != null)
      return "Illegal $where query";
    if (query.$mapReduce != null)
      return "Illegal $mapReduce query";
  }
};

function extractCursorMethods(query) {
  var out = [];
  for (var key in query) {
    if (cursorOperators[key]) {
      out.push([cursorOperators[key], query[key]]);
      delete query[key];
    }
  }
  return out;
}

function normalizeQuery(inputQuery) {
  // Box queries inside of a $query and clone so that we know where to look
  // for selctors and can modify them without affecting the original object
  var query;
  if (inputQuery.$query) {
    query = shallowClone(inputQuery);
    query.$query = shallowClone(query.$query);
  } else {
    query = {$query: {}};
    for (var key in inputQuery) {
      if (metaOperators[key] || cursorOperators[key]) {
        query[key] = inputQuery[key];
      } else {
        query.$query[key] = inputQuery[key];
      }
    }
  }

  // Deleted documents are kept around so that we can start their version from
  // the last version if they get recreated. When they are deleted, their type
  // is set to null, so don't return any documents with a null type.
  if (!query.$query._type) query.$query._type = {$ne: null};

  return query;
}

function cropDocument(doc, projection){
  var result = {};

  for (var key in doc) {
    if (!projection[key]){
      delete doc[key];
    }
  }

  return doc;
}

// The fields property is already pretty perfect for mongo. This will only work for JSON documents.
function projectionFromFields(fields) {
  var projection = {};
  for (var k in fields) {
    projection[k] = 1;
  }
  projection._v = 1;
  projection._type = 1;
  projection._m = 1;

  return projection;
}

function castToDoc(docName, data) {
  var doc = (
      typeof data.data === 'object' &&
      data.data !== null &&
      !Array.isArray(data.data)
      ) ?
      shallowClone(data.data) :
  {_data: (data.data === void 0) ? null : data.data};
  doc._type = data.type || null;
  doc._v = data.v;
  doc._m = data.m;
  doc._id = docName;
  return doc;
}

function castToSnapshot(doc) {
  if (!doc) return;
  var type = doc._type;
  var v = doc._v;
  var docName = doc._id;
  var data = doc._data;
  var meta = doc._m;
  if (data === void 0) {
    doc = shallowClone(doc);
    delete doc._type;
    delete doc._v;
    delete doc._id;
    delete doc._m;
    return {
      data: doc
      , type: type
      , v: v
      , docName: docName
      , m: meta
    };
  }
  return {
    data: data
    , type: type
    , v: v
    , docName: docName
    , m: meta
  };
}

function shallowClone(object) {
  var out = {};
  for (var key in object) {
    out[key] = object[key];
  }
  return out;
}
