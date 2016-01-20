module.exports = ShareDbMingo;

var Mingo = require('mingo');
var DB = require('sharedb').DB;
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

function ShareDbMingo(options) {
  if (!(this instanceof ShareDbMingo)) return new ShareDbMingo();

  this.docs = {};
  this.ops = {};

  options = options || {};

  this.allowJSQueries = options.allowAllQueries || options.allowJSQueries || false;
  this.allowAggregateQueries = options.allowAllQueries || options.allowAggregateQueries || false;

  this.closed = false;
}

ShareDbMingo.prototype = Object.create(DB.prototype);

ShareDbMingo.prototype.dropDatabase = function(callback) {
  var db = this;
  process.nextTick(function() {
    db.docs = {};
    db.ops = {};

    callback && callback();
  });
};

ShareDbMingo.prototype.commit = function(collection, id, op, snapshot, callback) {
  var db = this;
  process.nextTick(function() {
    var version = db._getVersionSync(collection, id);
    if (snapshot.v !== version + 1) {
      var succeeded = false;
      return callback(null, succeeded);
    }
    var err = db._writeOpSync(collection, id, op);
    if (err) return callback(err);
    err = db._writeSnapshotSync(collection, id, snapshot);
    if (err) return callback(err);
    var succeeded = true;
    callback(null, succeeded);
  });
};

ShareDbMingo.prototype._writeSnapshotSync = function(collection, id, snapshot) {
  var collectionDocs = this.docs[collection] || (this.docs[collection] = {});
  collectionDocs[id] = castToDoc(id, clone(snapshot));
};

ShareDbMingo.prototype._writeOpSync = function(collection, id, op) {
  var opLog = this._getOpLogSync(collection, id);
  // This will write an op in the log at its version, which should always be
  // the next item in the array under normal operation
  opLog[op.v] = clone(op);
};

ShareDbMingo.prototype._getVersionSync = function(collection, id) {
  var collectionOps = this.ops[collection];
  var version = (collectionOps && collectionOps[id] && collectionOps[id].length) || 0;
  return version;
};

// Snapshot database API

// Get the named document from the database. The callback is called with (err,
// data). data may be null if the docuemnt has never been created in the
// database.
ShareDbMingo.prototype.getSnapshot = function(collectionName, id, fields, callback) {
  var db = this;

  process.nextTick(function() {
    var snapshot = db._getSnapshotSync(collectionName, id, fields);
    callback(null, snapshot);
  });
};

ShareDbMingo.prototype._getSnapshotSync = function (collectionName, id){
  var collection = this.docs[collectionName];
  var doc = collection && collection[id];

  var snapshot = (doc) ? castToSnapshot(clone(doc)) : new MongoSnapshot(id, 0, null, undefined);
  return snapshot;
};

// ********* Oplog API

ShareDbMingo.prototype.getOps = function(collection, id, from, to, callback) {
  var db = this;
  process.nextTick(function() {
    var opLog = db._getOpLogSync(collection, id);
    if (to == null) {
      to = opLog.length;
    }
    var ops = clone(opLog.slice(from, to));
    callback(null, ops);
  });
};

ShareDbMingo.prototype._getOpLogSync = function(collection, id) {
  var collectionOps = this.ops[collection] || (this.ops[collection] = {});
  return collectionOps[id] || (collectionOps[id] = []);
};


// ********** Query support API.

// The memory database query function returns all documents in a collection
// regardless of query by default
ShareDbMingo.prototype.query = function(collection, query, fields, options, callback) {
  var db = this;
  process.nextTick(function() {
    var collectionDocs = db.docs[collection];
    var docs = [];
    for (var id in collectionDocs || {}) {
      var doc = db.docs[collection] && db.docs[collection][id];
      docs.push(clone(doc));
    }
    try {
      var data = db._querySync(docs, clone(query), options);
      callback(null, data.results || [], data.extra);
    } catch (err) {
      callback(err);
    }
  });
};

// For testing, it may be useful to implement the desired query language by
// defining this function
ShareDbMingo.prototype._querySync = function(docs, query, options) {
  if (query.$count) {
    delete query.$count;
    var cursor = Mingo.find(docs, query.$query);
    return  {results: [], extra: cursor.count()};

  } else if (query.$aggregate) {
    var aggregate = query.$aggregate;

    if (!Array.isArray(aggregate)) aggregate = [aggregate];

    var extra = Mingo.aggregate(docs, aggregate);
    return {results: [], extra: extra};
  } else {
    query = normalizeQuery(clone(query));
    var cursorMethods = extractCursorMethods(query);

    var cursor = Mingo.find(docs, query.$query);

    for (var i = 0; i < cursorMethods.length; i++) {
      var item = cursorMethods[i];
      var method = item[0];
      var arg = item[1];
      cursor[method](arg);
    }

    var results = cursor.all();
    results = results.map(function(doc){
      return castToSnapshot(doc);
    });
    return  {results: results};
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

function castToDoc(id, snapshot, opLink) {
  var data = snapshot.data;
  var doc =
    (isObject(data)) ? shallowClone(data) :
    (data === undefined) ? {} :
    {_data: data};
  doc._id = id;
  doc._type = snapshot.type;
  doc._v = snapshot.v;
  doc._m = snapshot.m;
  doc._o = opLink;
  return doc;
}

function castToSnapshot(doc) {
  var id = doc._id;
  var version = doc._v;
  var type = doc._type;
  var data = doc._data;
  var meta = doc._m;
  var opLink = doc._o;
  if (type == null) {
    return new MongoSnapshot(id, version, null, undefined, meta, opLink);
  }
  if (doc.hasOwnProperty('_data')) {
    return new MongoSnapshot(id, version, type, data, meta, opLink);
  }
  data = shallowClone(doc);
  delete data._id;
  delete data._v;
  delete data._type;
  delete data._m;
  delete data._o;
  return new MongoSnapshot(id, version, type, data, meta, opLink);
}
function MongoSnapshot(id, version, type, data, meta, opLink) {
  this.id = id;
  this.v = version;
  this.type = type;
  this.data = data;
  if (meta) this.m = meta;
  if (opLink) this._opLink = opLink;
}

function isObject(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function shallowClone(object) {
  var out = {};
  for (var key in object) {
    out[key] = object[key];
  }
  return out;
}
