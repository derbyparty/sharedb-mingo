var expect = require('expect.js');
var ShareDbMingo = require('./index');

function create(callback) {
  var db = ShareDbMingo();
  db.dropDatabase(function(){
    callback(null, db);
  });
}

require('sharedb/test/db')(create);

describe('mongo db', function() {
  beforeEach(function(done) {
    var self = this;
    create(function(err, db, mongo) {
      if (err) throw err;
      self.db = db;
      self.mongo = mongo;
      done();
    });
  });

  afterEach(function(done) {
    this.db.close(done);
  });

  describe('query', function() {

    it('$aggregate should perform aggregate command', function(done) {
      var snapshots = [
        {type: 'json0', v: 1, data: {x: 1, y: 1}},
        {type: 'json0', v: 1, data: {x: 2, y: 2}},
        {type: 'json0', v: 1, data: {x: 3, y: 2}}
      ];
      var db = this.db;
      db.allowAggregateQueries = true;
      db.commit('testcollection', 'test1', {v: 0, create: {}}, snapshots[0], function(err) {
        db.commit('testcollection', 'test2', {v: 0, create: {}}, snapshots[1], function(err) {
          db.commit('testcollection', 'test3', {v: 0, create: {}}, snapshots[2], function(err) {
            var query = {$aggregate: [
              {$group: {_id: '$y', count: {$sum: 1}}},
              {$sort: {count: 1}}
            ]};
            db.query('testcollection', query, null, null, function(err, results, extra) {
              if (err) throw err;
              expect(extra).eql([{_id: 1, count: 1}, {_id: 2, count: 2}]);
              done();
            });
          });
        });
      });
    });

  });
});