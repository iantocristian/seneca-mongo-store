/* Copyright (c) 2010-2013 Richard Rodger, MIT License */
"use strict";

module.exports = MongoStore;

var EventEmitter = require('events').EventEmitter;

var _     = require('underscore');
var mongo = require('mongodb');

var STORE_NAME  = "mongo-store";


function MongoStore(seneca) {
  if (!(this instanceof MongoStore))
      return new MongoStore(seneca);

  EventEmitter.call(this)

  var self = this;

  self.name = STORE_NAME;
  self.description = [];

  self.seneca = seneca;

  self.errorHandler = function(args, err, cb) {
    seneca.log.debug('error: ' + err)
    seneca.fail({code: 'entity/error', store: self.name}, cb)
  }

  self.dbInstance = null;
  self.dbCollectionMap = {};
}

MongoStore.prototype = Object.create(
  EventEmitter.prototype, { constructor: { value: MongoStore }}
)


MongoStore.prototype.save = function (args, cb) {
  var self = this;

  var ent = args.ent;

  var update = !!ent.id;

  getCollection(self, ent, function (err, dbCollection) {
    if (!self.errorHandler.call(self, args, err, cb)) {
      var entp = {};

      var fields = ent.fields$()
      fields.forEach(function (field) {
        entp[field] = ent[field]
      })

      if (update) {
        var q = {_id: makeId(ent.id)}
        delete entp.id

        dbCollection.update(q, entp, {upsert: true}, function (err, update) {
          if (!self.errorHandler.call(self, args, err, cb)) {
            self.seneca.log.debug('save/update', ent, self.description)
            cb(null, ent)
          }
        })
      }
      else {
        dbCollection.insert(entp, function (err, inserts) {
          if (!self.errorHandler.call(self, args, err, cb)) {
            ent.id = inserts[0]._id.toHexString()

            self.seneca.log.debug('save/insert', ent, self.description)
            cb(null, ent)
          }
        })
      }
    }
  })
}

MongoStore.prototype.load = function (args, cb) {
  var self = this;

  var qent = args.qent;
  var q = args.q;

  getCollection(self, qent, function (err, dbCollection) {
    if (!self.errorHandler.call(self, args, err, cb)) {
      var mq = metaQuery(q)
      var qq = fixQuery(q)

      dbCollection.findOne(qq, mq, function (err, dbItem) {
        if (!self.errorHandler.call(self, args, err, cb)) {
          var entity = makeEntity(self, qent, dbItem);
          self.seneca.log.debug('load', q, entity, self.description)
          cb(null, entity);
        }
      });
    }
  })
}

MongoStore.prototype.list = function (args, cb) {
  var self = this;

  var qent = args.qent;
  var q = args.q;

  getCollection(self, qent, function (err, dbCollection) {
    if (!self.errorHandler.call(self, args, err, cb)) {
      var mq = metaQuery(q)
      var qq = fixQuery(q)

      dbCollection.find(qq, mq, function (err, dbCursor) {
        if (!self.errorHandler.call(self, args, err, cb)) {
          var list = []

          dbCursor.each(function (err, dbItem) {
            if (!self.errorHandler.call(self, args, err, cb)) {
              if (dbItem) {
                list.push(makeEntity(self, qent, dbItem))
              }
              else {
                self.seneca.log.debug('list', q, list.length, list[0], self.description)
                cb(null, list)
              }
            }
          })
        }
      })
    }
  })
}

MongoStore.prototype.find = function (args, cb) {
  var self = this;

  var qent = args.qent;
  var q = args.q;

  getCollection(self, qent, function (err, dbCollection) {
    if (!self.errorHandler.call(self, args, err, cb)) {
      var mq = metaQuery(q)
      var qq = fixQuery(q)

      dbCollection.find(qq, mq, function (err, dbCursor) {
        if (!self.errorHandler.call(self, args, err, cb)) {
          cb(null, dbCursor);
        }
      })
    }
  })
}

MongoStore.prototype.remove = function (args, cb) {
  var self = this;

  var qent = args.qent;
  var q = args.q;

  var all = q.all$; // default false
  var load = _.isUndefined(q.load$) ? true : q.load$; // default true

  getCollection(self, qent, function (err, dbCollection) {
    if (!self.errorHandler.call(self, args, err, cb)) {
      var qq = fixQuery(q);

      if (all) {
        dbCollection.remove(qq, function (err) {
          self.seneca.log.debug('remove/all', q, self.description);
          cb(err)
        })
      }
      else {
        var mq = metaQuery(q)
        dbCollection.findOne(qq, mq, function (err, dbItem) {
          if (!self.errorHandler.call(self, args, err, cb)) {
            if (dbItem) {
              dbCollection.remove({_id: dbItem._id}, function (err) {
                self.seneca.log.debug('remove/one', q, dbItem, self.description);

                var ent = load ? dbItem : null
                cb(err, ent)
              })
            }
            else cb(null)
          }
        })
      }
    }
  })
}

MongoStore.prototype.native = function (args, cb) {
  var self = this;

  // TOREVIEW: need this dummy call to ensure db connection ???
  self.dbInstance.collection('seneca', function (err, dbCollection) {
    if (!self.errorHandler.call(self, args, err, cb)) {
      dbCollection.findOne({}, {}, function (err) {
        if (!self.errorHandler.call(self, args, err, cb)) {
          cb(null, self.dbInstance)
        }
      })
    }
  })
}

MongoStore.prototype.close = function (cb) {
  var self = this;

  if (self.dbInstance) {
    self.dbInstance.close(cb)
  }
}


MongoStore.prototype.setDescription = function(description) {
  this.description = description;
}

MongoStore.prototype.setErrorHandler = function(errorHandler) {
  this.errorHandler = errorHandler;
}

MongoStore.prototype.setDbInstance = function(dbInstance) {
  this.dbInstance = dbInstance;
}


function getCollection(self, qent, cb) {

  var canon = qent.canon$({object: true});
  var collectionName = (canon.base ? canon.base + '_' : '') + canon.name;

  if (!self.dbCollectionMap[collectionName]) {
    self.dbInstance.collection(collectionName, function (err, dbCollection) {
      if (err) {
        return cb(err)
      }
      else {
        self.dbCollectionMap[collectionName] = dbCollection;
        cb(null, dbCollection)
      }
    })
  }
  else {
    cb(null, self.dbCollectionMap[collectionName])
  }
}

function makeEntity(self, qent, dbItem) {
  if (dbItem) {
    dbItem.id = dbItem._id.toHexString();
    delete dbItem._id;

    return qent.make$(dbItem);
  }
  else {
    return null;
  }
}


/*
native$ = object => use object as query, no meta settings
native$ = array => use first elem as query, second elem as meta settings
*/

function makeId(hexstr) {
  if( mongo.BSONNative ) {
    return new mongo.BSONNative.ObjectID(hexstr)
  }
  else {
    return new mongo.BSONPure.ObjectID(hexstr)
  }
}

function fixQuery(q) {
  var qq = {};

  if (!q.native$) {
    for (var qp in q) {
      if (!qp.match(/\$$/)) {
        qq[qp] = q[qp]
      }
    }
    if (qq.id) {
      qq._id = makeId(qq.id)
      delete qq.id
    }
  }
  else {
    qq = _.isArray(q.native$) ? q.native$[0] : q.native$
  }

  return qq
}

function metaQuery(q) {
  var mq = {}

  if (!q.native$) {

    if (q.sort$) {
      for (var sf in q.sort$) break;
      var sd = q.sort$[sf] < 0 ? 'descending' : 'ascending'
      mq.sort = [
        [sf, sd]
      ]
    }

    if (q.limit$) {
      mq.limit = q.limit$
    }

    if (q.skip$) {
      mq.skip = q.skip$
    }

    if (q.fields$) {
      mq.fields = q.fields$
    }
  }
  else {
    mq = _.isArray(q.native$) ? q.native$[1] : mq
  }

  return mq
}

