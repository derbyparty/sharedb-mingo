# Mocha test using livedb's snapshot tests
liveDbMongo = require '../lib'
assert = require 'assert'

# Clear mongo
clear = (callback) ->
#  mongo = mongoskin.db 'mongodb://localhost:27017/test?auto_reconnect', safe:true
#  mongo.dropCollection 'testcollection', ->
#    mongo.dropCollection 'testcollection_ops', ->
#      mongo.close()

      callback()

create = (callback) ->
  clear ->
    callback liveDbMongo()
#    callback liveDbMongo 'mongodb://localhost:27017/test?auto_reconnect', safe: false

describe 'mongo', ->
  afterEach clear

  describe 'raw', ->
    beforeEach (done) ->
#      @mongo = mongoskin.db 'mongodb://localhost:27017/test?auto_reconnect', safe:true
      create (@db) => done()

    afterEach ->
#      @mongo.close()


    describe 'query', ->
      it 'returns data in the collection', (done) ->
        snapshot = {type:'json0', v:5, m:{}, data:{x:5, y:6}}
        @db.writeSnapshot 'testcollection', 'test', snapshot, (err) =>
          @db.query 'unused', 'testcollection', {x:5}, {}, (err, results) ->
            throw Error err if err
            delete results[0].docName
            assert.deepEqual results, [snapshot]
            done()

      it 'returns nothing when there is no data', (done) ->
        @db.query 'unused', 'testcollection', {x:5}, {}, (err, results) ->
          throw Error err if err
          assert.deepEqual results, []
          done()

      it 'does not allow $where queries', (done) ->
        @db.query 'unused', 'testcollection', {$where:"true"}, {}, (err, results) ->
          assert.ok err
          assert.equal results, null
          done()

      it.skip 'distinct should return distinct data', (done) ->
        snapshots = [
          {type:'json0', v:5, m:{}, data:{x:1, y:1}},
          {type:'json0', v:5, m:{}, data:{x:2, y:2}},
          {type:'json0', v:5, m:{}, data:{x:3, y:2}}
        ]
        @db.writeSnapshot 'testcollection', 'test1', snapshots[0], (err) =>
          @db.writeSnapshot 'testcollection', 'test2', snapshots[1], (err) =>
            @db.writeSnapshot 'testcollection', 'test3', snapshots[2], (err) =>
              @db.query 'unused', 'testcollection', {$distinct: true, $field: 'y', $query: {}}, {}, (err, results) ->
                throw Error err if err
                assert.deepEqual results.extra, [1,2]
                done()

      it 'does not allow $mapReduce queries by default', (done) ->
        snapshots = [
          {type:'json0', v:5, m:{}, data:{player: 'a', round: 1, score: 5}},
          {type:'json0', v:5, m:{}, data:{player: 'a', round: 2, score: 7}},
          {type:'json0', v:5, m:{}, data:{player: 'b', round: 1, score: 15}}
        ]
        @db.writeSnapshot 'testcollection', 'test1', snapshots[0], (err) =>
          @db.writeSnapshot 'testcollection', 'test2', snapshots[1], (err) =>
            @db.writeSnapshot 'testcollection', 'test3', snapshots[2], (err) =>
              @db.query 'unused', 'testcollection',
                $mapReduce: true,
                $map: () ->
                  emit @.player, @score
                $reduce: (key, values) ->
                  values.reduce (t, s) -> t + s
                $query: {}
              , {}, (err, results) ->
                assert.ok err
                assert.equal results, null
                done()

      it.skip '$mapReduce queries should work when allowJavaScriptQuery == true', (done) ->
        snapshots = [
          {type:'json0', v:5, m:{}, data:{player: 'a', round: 1, score: 5}},
          {type:'json0', v:5, m:{}, data:{player: 'a', round: 2, score: 7}},
          {type:'json0', v:5, m:{}, data:{player: 'b', round: 1, score: 15}}
        ]
        @db.allowJavaScriptQuery = true

        @db.writeSnapshot 'testcollection', 'test1', snapshots[0], (err) =>
          @db.writeSnapshot 'testcollection', 'test2', snapshots[1], (err) =>
            @db.writeSnapshot 'testcollection', 'test3', snapshots[2], (err) =>
              @db.query 'unused', 'testcollection',
                $mapReduce: true,
                $map: () ->
                  emit @.player, @score
                $reduce: (key, values) ->
                  values.reduce (t, s) -> t + s
                $query: {}
              , {}, (err, results) ->
                throw Error err if err
                assert.deepEqual results.extra, [{_id: 'a', value: 12}, {_id: 'b', value: 15}]
                done()

    describe 'queryProjected', ->
      it 'returns only projected fields', (done) ->
        @db.writeSnapshot 'testcollection', 'test', {type:'json0', v:5, m:{}, data:{x:5, y:6}}, (err) =>
          @db.queryProjected 'unused', 'testcollection', {y:true}, {x:5}, {}, (err, results) ->
            throw Error err if err
            assert.deepEqual results, [{type:'json0', v:5, m:{}, data:{y:6}, docName:'test'}]
            done()

      it 'returns no data for matching documents if fields is empty', (done) ->
        @db.writeSnapshot 'testcollection', 'test', {type:'json0', v:5, m:{}, data:{x:5, y:6}}, (err) =>
          @db.queryProjected 'unused', 'testcollection', {}, {x:5}, {}, (err, results) ->
            throw Error err if err
            assert.deepEqual results, [{type:'json0', v:5, m:{}, data:{}, docName:'test'}]
            done()

    describe 'queryDoc', ->
      it 'returns null when the document does not exist', (done) ->
        @db.queryDoc 'unused', 'unused', 'testcollection', 'doesnotexist', {}, (err, result) ->
          throw Error err if err
          assert.equal result, null
          done()

      it 'returns the doc when the document does exist', (done) ->
        snapshot = {type:'json0', v:5, m:{}, data:{x:5, y:6}}
        @db.writeSnapshot 'testcollection', 'test', snapshot, (err) =>
          @db.queryDoc 'unused', 'unused', 'testcollection', 'test', {}, (err, result) ->
            throw Error err if err
            snapshot.docName = 'test'
            assert.deepEqual result, snapshot
            done()

      it 'does not allow $where queries', (done) ->
        @db.queryDoc 'unused', 'unused', 'testcollection', 'somedoc', {$where:"true"}, (err, result) ->
          assert.ok err
          assert.equal result, null
          done()

    describe 'queryDocProjected', ->
      beforeEach (done) ->
        @snapshot = {type:'json0', v:5, m:{}, data:{x:5, y:6}}
        @db.writeSnapshot 'testcollection', 'test', @snapshot, (err) =>
          @snapshot.docName = 'test'
          throw Error err if err
          done()

      it 'returns null when the document does not exist', (done) ->
        @db.queryDocProjected 'unused', 'unused', 'testcollection', 'doesnotexist', {x:true}, {}, (err, result) ->
          throw Error err if err
          assert.equal result, null
          done()

      it 'returns the requested fields of the doc', (done) ->
        @db.queryDocProjected 'unused', 'unused', 'testcollection', 'test', {x:true}, {}, (err, result) =>
          throw Error err if err
          @snapshot.data = {x:5}
          assert.deepEqual result, @snapshot
          done()

      it 'returns empty data if no fields are requested', (done) ->
        @db.queryDocProjected 'unused', 'unused', 'testcollection', 'test', {}, {}, (err, result) =>
          throw Error err if err
          @snapshot.data = {}
          assert.deepEqual result, @snapshot
          done()

    describe 'spec query', ->
      it '$skip', (done) ->
        snapshots = [
          {type:'json0', v:5, m:{}, data:{player: 'a', round: 1, score: 5}},
          {type:'json0', v:5, m:{}, data:{player: 'a', round: 2, score: 2}},
          {type:'json0', v:5, m:{}, data:{player: 'b', round: 1, score: 15}}
        ]
        @db.writeSnapshot 'testcollection', 'test1', snapshots[0], (err) =>
          @db.writeSnapshot 'testcollection', 'test2', snapshots[1], (err) =>
            @db.writeSnapshot 'testcollection', 'test3', snapshots[2], (err) =>
              @db.query 'unused', 'testcollection',
                player: 'a'
                $skip: 1
              , {}, (err, results) ->
#                console.log('res', err);
                assert.equal results.length, 1
                assert.equal results[0].data.round, 2
                done()

      it '$limit', (done) ->
        snapshots = [
          {type:'json0', v:5, m:{}, data:{player: 'a', round: 1, score: 5}},
          {type:'json0', v:5, m:{}, data:{player: 'a', round: 2, score: 2}},
          {type:'json0', v:5, m:{}, data:{player: 'b', round: 1, score: 15}}
        ]
        @db.writeSnapshot 'testcollection', 'test1', snapshots[0], (err) =>
          @db.writeSnapshot 'testcollection', 'test2', snapshots[1], (err) =>
            @db.writeSnapshot 'testcollection', 'test3', snapshots[2], (err) =>
              @db.query 'unused', 'testcollection',
                player: 'a'
                $limit: 1
              , {}, (err, results) ->
#                console.log('res', results);
                assert.equal results.length, 1
                assert.equal results[0].data.round, 1
                done()
      it '$orderby', (done) ->
        snapshots = [
          {type:'json0', v:5, m:{}, data:{player: 'a', round: 1, score: 5}},
          {type:'json0', v:5, m:{}, data:{player: 'a', round: 2, score: 2}},
          {type:'json0', v:5, m:{}, data:{player: 'b', round: 1, score: 15}}
        ]
        @db.writeSnapshot 'testcollection', 'test1', snapshots[0], (err) =>
          @db.writeSnapshot 'testcollection', 'test2', snapshots[1], (err) =>
            @db.writeSnapshot 'testcollection', 'test3', snapshots[2], (err) =>
              @db.query 'unused', 'testcollection',
                $orderby: {score: 1}
              , {}, (err, results) ->
#                console.log('res', results);
                assert.equal results.length, 3
                assert.equal results[0].data.score, 2
                assert.equal results[1].data.score, 5
                assert.equal results[2].data.score, 15
                done()
      it '$count', (done) ->
        snapshots = [
          {type:'json0', v:5, m:{}, data:{player: 'a', round: 1, score: 5}},
          {type:'json0', v:5, m:{}, data:{player: 'a', round: 2, score: 2}},
          {type:'json0', v:5, m:{}, data:{player: 'b', round: 1, score: 15}}
        ]
        @db.writeSnapshot 'testcollection', 'test1', snapshots[0], (err) =>
          @db.writeSnapshot 'testcollection', 'test2', snapshots[1], (err) =>
            @db.writeSnapshot 'testcollection', 'test3', snapshots[2], (err) =>
              @db.query 'unused', 'testcollection',
                $count: true
                $query: {}
              , {}, (err, results) ->
#                console.log('res', results);
                assert.equal results.extra, 3
                done()

  require('livedb/test/snapshotdb') create
  require('livedb/test/oplog') create

