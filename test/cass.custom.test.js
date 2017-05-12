// Copyright IBM Corp. 2017. All Rights Reserved.
// Node module: loopback-connector-cassandra
// This file is licensed under the MIT License.
// License text available at https://opensource.org/licenses/MIT

// This test written in mocha+should.js
'use strict';

/* global getSchema:false */

var cassandra = require('cassandra-driver');
var should = require('should');

var db, CASS, CASS_SORTABLE, CASS_TUPLE_TIME;
var ID, ROW;
var cassTestString = 'cassandra test string data';
var cassTestNum = 3;

describe('cassandra custom tests', function() {
  before(function(done) {
    db = getSchema();
    CASS = db.define('CASS', {
      str: String,
      num: Number,
      });
    CASS_SORTABLE = db.define('CASS_SORTABLE', {
      patBool: {type: Boolean, id: 2},
      str: String,
      patStr: {type: String, id: true},
      num: Number,
      patNum: {type: Number, id: 1},
      yearMonth: {type: String, index: true},
      }, {
      cassandra: {
        clusteringKeys: ['str', 'num DESC'],
        },
      });
    CASS_TUPLE_TIME = db.define('CASS_TUPLE_TIME', {
      tuple: {type: 'Tuple'},
      str: String,
      num: Number,
      time: {type: 'TimeUuid', id: true},
      });
    db.automigrate(['CASS', 'CASS_SORTABLE', 'CASS_TUPLE_TIME'], done);
  });

  function verifyTheDefaultRows(err, m) {
    should.not.exists(err);
    should.exist(m && m.id);
    should.exist(m && m.str);
    should.exist(m && m.num);
    m.str.should.be.type('string');
    m.str.indexOf(cassTestString).should.be.aboveOrEqual(0);
    m.num.should.be.type('number');
    m.num.should.be.aboveOrEqual(cassTestNum);    
 }

  function verifyExtraRows(err, m) {
    should.not.exists(err);
    should.exist(m && m.patBool);
    should.exist(m && m.patNum);
    should.exist(m && m.patStr);
    m.patBool.should.be.type('boolean');
    m.patBool.should.equal(true);
    m.patNum.should.be.type('number');
    m.patNum.should.equal(100);
    m.patStr.should.be.type('string');
    m.patStr.should.equal(cassTestString + '100');
 }


  // http://apidocs.strongloop.com/loopback/#persistedmodel-create
  it('create', function(done) {
    CASS.create({
      str: cassTestString,
      num: cassTestNum,
    }, function(err, m) {
      verifyTheDefaultRows(err, m);
      ROW = m;
      ID = m.id;
      done();
    });
  });

  // http://apidocs.strongloop.com/loopback/#persistedmodel-findbyid
  it('findOne', function(done) {
    CASS.findOne({where: {id: ID}}, function(err, m) {
      verifyTheDefaultRows(err, m);
      done();
    });
  });

  // http://apidocs.strongloop.com/loopback/#persistedmodel-findbyid
  it('findById', function(done) {
    CASS.findById(ID, function(err, m) {
      verifyTheDefaultRows(err, m);
      done();
    });
  });

  // http://apidocs.strongloop.com/loopback/#persistedmodel-prototype-updateattributes
  it('destroyAll with id', function(done) {
    CASS.destroyAll({id: ID},
    function(err, info) {
      should.not.exist(err);
      should.exist(info);
      should.not.exist(info.count);
      // however, info.count is not set. :-(
      // info.count.should.be.type('number');
      done();
    });
  });

  // http://apidocs.strongloop.com/loopback/#persistedmodel-create
  it('re-create', function(done) {
    CASS.create({
      str: cassTestString,
      num: cassTestNum,
    }, function(err, m) {
      verifyTheDefaultRows(err, m);
      ROW = m;
      ID = m.id;
      done();
    });
  });

  // http://apidocs.strongloop.com/loopback/#persistedmodel-prototype-updateattributes
  it('updateAttributes', function(done) {
    ROW.updateAttributes({
      id: ID, str: cassTestString + '2', num: cassTestNum + 1,
    }, function(err, instance) {
      should.not.exists(err);
      instance.id.should.be.equal(ID);
      instance.str.should.be.type('string');;
      instance.str.should.be.equal(cassTestString + '2');;
      instance.num.should.be.type('number');;
      instance.num.should.be.equal(cassTestNum + 1);;
      done();
    });
  });

  // http://apidocs.strongloop.com/loopback/#persistedmodel-destroyall
  it('destroyAll without id', function(done) {
    CASS.destroyAll({},
    function(err, info) {
      should.not.exist(err);
      should.not.exist(info.count);
      // however, info.count is not set. :-(
      // info.count.should.be.type('number');
      done();
    });
  });

  var ID_1;

  it('create sortable 1', function(done) {
    CASS_SORTABLE.create({
      str: cassTestString + '10',
      num: 10,
      patBool: true,
      patNum: 100,
      patStr: cassTestString + '100',
    }, function(err, m) {
      verifyTheDefaultRows(err, m);
      verifyExtraRows(err, m);
      m.id.should.not.have.property('id');
      m.id.should.have.properties(
        {patStr: 'cassandra test string data100',
        patNum: 100,
        patBool: true });
      ID_1 = m.id;
      done();
    });
  });

  it('create sortable 2', function(done) {
    CASS_SORTABLE.create({
      str: cassTestString + '20',
      num: 20,
      patBool: true,
      patNum: 100,
      patStr: cassTestString + '100',
    }, function(err, m) {
      verifyTheDefaultRows(err, m);
      verifyExtraRows(err, m);
      m.id.should.have.properties(ID_1);
      m.id.should.not.have.property('id');
      m.id.should.have.properties(
        {patStr: 'cassandra test string data100',
        patNum: 100,
        patBool: true });
      done();
    });
  });

  it('create sortable 3', function(done) {
    CASS_SORTABLE.create({
      str: cassTestString + '20',
      num: 30,
      patBool: true,
      patNum: 100,
      patStr: cassTestString + '100',
    }, function(err, m) {
      verifyTheDefaultRows(err, m);
      verifyExtraRows(err, m);
      m.id.should.have.properties(ID_1);
      m.id.should.not.have.property('id');
      m.id.should.have.properties(
        {patStr: 'cassandra test string data100',
        patNum: 100,
        patBool: true });
      done();
    });
  });

  it('find and order by str', function(done) {
    CASS_SORTABLE.find(
      {where: {id: ID_1},
      order: 'str'}, function(err, rows) {
        should.not.exist(err);
        rows.should.have.length(3); // str ASC
        rows[0].str.should.eql('cassandra test string data10');
        rows[1].str.should.eql('cassandra test string data20');
        rows[2].str.should.eql('cassandra test string data20');
        done();
      });
  });

  it('find and order by num', function(done) {
    CASS_SORTABLE.find(
      {where: {and: [{id: ID_1},{str: 'cassandra test string data20'}]},
      order: 'num'}, function(err, rows) {
        should.not.exist(err);
        rows.should.have.length(2); // num DESC
        rows[0].num.should.eql(30);
        rows[1].num.should.eql(20);
        done();
      });
  });

  it('create sortable 4', function(done) {
    CASS_SORTABLE.create({
      str: cassTestString + '50',
      num: 40,
      patBool: true,
      patNum: 100,
      patStr: cassTestString + '100',
      yearMonth: '2015-03',
    }, function(err, m) {
      verifyTheDefaultRows(err, m);
      verifyExtraRows(err, m);
      m.id.should.have.properties(ID_1);
      m.id.should.not.have.property('id');
      m.should.have.properties({yearMonth: '2015-03'});
      done();
    });
  });

  it('create sortable 5', function(done) {
    CASS_SORTABLE.create({
      str: cassTestString + '50',
      num: 50,
      patBool: true,
      patNum: 100,
      patStr: cassTestString + '100',
      yearMonth: '2015-04',
    }, function(err, m) {
      verifyTheDefaultRows(err, m);
      verifyExtraRows(err, m);
      m.id.should.have.properties(ID_1);
      m.id.should.not.have.property('id');
      m.should.have.properties({yearMonth: '2015-04'});
      done();
    });
  });

  it('create sortable 6', function(done) {
    CASS_SORTABLE.create({
      str: cassTestString + '50',
      num: 60,
      patBool: true,
      patNum: 100,
      patStr: cassTestString + '100',
      yearMonth: '2015-04',
    }, function(err, m) {
      verifyTheDefaultRows(err, m);
      verifyExtraRows(err, m);
      m.id.should.have.properties(ID_1);
      m.id.should.not.have.property('id');
      m.should.have.properties({yearMonth: '2015-04'});
      done();
    });
  });

  it('find by secondary key without primary key', function(done) {
    CASS_SORTABLE.find(
      {where: {yearMonth: '2015-04'}}, function(err, rows) {
        should.not.exist(err);
        rows.should.have.length(2); // num DESC
        rows[0].str.should.eql(cassTestString + '50');
        rows[1].str.should.eql(rows[0].str);
        rows[0].num.should.be.eql(60);
        rows[1].num.should.be.eql(50);
        done();
      });
  });

  var ID_2, savedTuple, savedTimeUuid;
  var origTupleArray = ['USA', 'California', 'San Francisco', 'Market St.'];
  var origTuple = cassandra.types.Tuple.fromArray(origTupleArray);
  var origTimeUuid = cassandra.types.TimeUuid.now();

  it('create tutple and timeuuid', function(done) {
    CASS_TUPLE_TIME.create({
      tuple: origTuple.values(),
      time: origTimeUuid.toString(),
      str: cassTestString,
      num: cassTestNum,
    }, function(err, m) {
      verifyTheDefaultRows(err, m);
      var mTupleArray = m.tuple.values();
      mTupleArray.should.be.instanceof(Array);
      mTupleArray.should.containDeep(origTupleArray);
      origTupleArray.should.containDeep(mTupleArray);
      savedTuple = m.tuple;
      var mTimeString = m.time.toString();
      mTimeString.should.be.instanceof(String);
      m.time.should.eql(origTimeUuid);
      savedTimeUuid = m.time;
      ID_2 = m.id;
      done();
    });
  });

  it('find by id tuple and timeuuid', function(done) {
    CASS_TUPLE_TIME.findById(savedTimeUuid, function(err, m) {
      should.not.exist(err);
      m.time.should.eql(savedTimeUuid);
      m.tuple.should.eql(savedTuple);
      done();
    });
  });

  it('find by tuple and timeuuid', function(done) {
    CASS_TUPLE_TIME.find(
      {where: {time: savedTimeUuid}}, function(err, m) {
      should.not.exist(err);
      m[0].time.should.eql(savedTimeUuid);
      m[0].tuple.should.eql(savedTuple);
      done();
    });
  });

  var targetTable = 'CASS_SORTABLE';

  it('discoverSchemas', function(done) {
    db.discoverSchemas(targetTable, {}, function(err, data) {
      console.log('============== discoverSchemas:', err, data);
      done(err);
    });
  });

  it('discoverSchema', function(done) {
    db.discoverSchema(targetTable, {}, function(err, data) {
      console.log('============== discoverSchema:', err, data);
      done(err);
    });
  });

  it('discoverPrimaryKeys', function(done) {
    db.discoverPrimaryKeys(targetTable, {}, function(err, data) {
      console.log('============== discoverPrimaryKeys:', err, data);
      done(err);
    });
  });

  it('discoverForeignKeys', function(done) {
    db.discoverForeignKeys(targetTable, {}, function(err, data) {
      console.log('============== discoverForeignKeys:', err, data);
      done(err);
    });
  });

  it('discoverExportedForeignKeys', function(done) {
    db.discoverExportedForeignKeys(targetTable, {}, function(err, data) {
      console.log('============== discoverExportedForeignKeys:', err, data);
      done(err);
    });
  });

  it('discoverAndBuildModels', function(done) {
    db.discoverAndBuildModels(targetTable, {}, function(err, data) {
      console.log('============== discoverAndBuildModels:', err, data);
      done(err);
    });
  });

  it('discoverModelDefinitions', function(done) {
    db.discoverModelDefinitions({}, function(err, data) {
      console.log('============== discoverModelDefinitions:', err, data);
      done(err);
    });
  });

  it('discoverModelProperties', function(done) {
    db.discoverModelProperties(targetTable, {}, function(err, data) {
      console.log('============== discoverModelProperties:', err, data);
      done(err);
    });
  });

});
