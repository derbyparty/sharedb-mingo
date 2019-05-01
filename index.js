module.exports = ShareDbMingo;

var Mingo = require('mingo');
var DB = require('sharedb').DB;
var clone = require('clone');
var uuid = require('uuid/v1')
var memoryStore = require('@js-code/memory-store')

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

function CollectionStore (store, prefix) {
  this.store = store
  this.prefix = prefix
}

CollectionStore.prototype.getCollectionDocs = function (collection) {
  return this.store.getItemIds(collection).then((ids) => {
    const promises = ids.filter(id => {
      const split = id.split('.')
      return split.length === 3 && split[0] === this.prefix && split[1] === collection
    }).map(id => (this.store.getItem(id)))
    return Promise.all(promises)
  })
}

CollectionStore.prototype.getDoc = function (collection, docId) {
  return this.store.getItem(`${this.prefix}.${collection}.${docId}`)
}

CollectionStore.prototype.setDoc = function (collection, docId, doc) {
  return this.store.setItem(`${this.prefix}.${collection}.${docId}`, doc)
}

function ShareDbMingo(options) {
  if (!(this instanceof ShareDbMingo)) return new ShareDbMingo();

  options = options || {};
  this.store = new CollectionStore(options.store || new memoryStore(), options.storePrefix || 'store')

  this.allowJSQueries = options.allowAllQueries || options.allowJSQueries || false;
  this.allowAggregateQueries = options.allowAllQueries || options.allowAggregateQueries || false;

  this.closed = false;
}

ShareDbMingo.prototype = Object.create(DB.prototype);

ShareDbMingo.prototype.dropDatabase = function(callback) {
  var db = this;
  db.store.clean().then(function() {
    callback && callback();
  })
};

ShareDbMingo.prototype.commit = function(collection, id, op, snapshot, options, callback) {
  var db = this;
  this._writeOp(collection, id, op, snapshot, (error, opId) => {
    if (error) return callback(error)
    db.store.setDoc(collection, id, castToDoc(id, clone(snapshot), opId)).then(function () {
      var succeeded = true;
      callback(null, succeeded);
    }).catch(err => callback(err))
  })

}

ShareDbMingo.prototype._writeOp = function(collectionName, id, op, snapshot, callback) {
  if (typeof op.v !== 'number') {
    var err = 'Invalid op version ' + collectionName + '.' + id + ' ' + op.v
    return callback(err);
  }

  const opCollectionName = this.getOplogCollectionName(collectionName)

  var doc = shallowClone(op);
  doc.d = id;
  doc.o = snapshot._opLink
  doc._id = uuid()
  this.store.setDoc(opCollectionName, doc._id, doc).then(() => {
    callback(null, doc._id)
  }).catch((error) => {
    callback(error)
  })
}


ShareDbMingo.prototype.getOplogCollectionName = function(collectionName) {
  return 'o_' + collectionName;
}

// Snapshot database API

// Get the named document from the database. The callback is called with (err,
// data). data may be null if the docuemnt has never been created in the
// database.
ShareDbMingo.prototype.getSnapshot = function(collectionName, id, fields, options, callback) {
  var db = this;
  db.store.getDoc(collectionName, id, fields).then((doc) => {
    var snapshot = (doc) ? castToSnapshot(clone(doc)) : new MongoSnapshot(id, 0, null, undefined);
    callback(null, snapshot);
  }).catch(function(error) {
    callback(error)
  })
};

// ********* Oplog API

function getOpsQuery(id, from) {
  return (from == null) ?
    {d: id} :
    {d: id, v: {$gte: from}};
}

ShareDbMingo.prototype.getOps = function(collection, id, from, to, options, callback) {
  var db = this;
  const opCollectionName = this.getOplogCollectionName(collection)

  this.store.getCollectionDocs(opCollectionName).then(function (docs) {
    docs = docs.map(function(doc) {
      return clone(doc)
    })

    var query = getOpsQuery(id, from);
    query['$sort'] = {v: 1}
    try {
      var data = db._querySync(docs, query, options);
      callback(null, data.results || []);
    } catch (err) {
      callback(err);
    }
  }).catch(err => (callback(err)));
};

// ********** Query support API.

// The memory database query function returns all documents in a collection
// regardless of query by default
ShareDbMingo.prototype.query = function(collection, query, fields, options, callback) {
  var db = this;
  this.store.getCollectionDocs(collection).then(function (docs) {
    docs = docs.map(function(doc) {
      return clone(doc)
    })
    try {
      var data = db._querySync(docs, clone(query), options);
      callback(null, data.results || [], data.extra);
    } catch (err) {
      callback(err);
    }
  }).catch(err => (callback(err)));
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
