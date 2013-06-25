/* Copyright (c) 2010-2013 Richard Rodger, MIT License */
"use strict";

var MIN_WAIT = 16
var MAX_WAIT = 65536

var Store = require('./store')

module.exports = function(seneca, opts, cb) {

  opts.minwait = opts.minwait || MIN_WAIT;
  opts.maxwait = opts.maxwait || MAX_WAIT;

  // autoreconnect defaults to true; set autoreconnect to false or 0 to disable
  // if autoreconnect is 2 both driver auto-reconnect and custom reconnect implementation are enabled
  opts.autoreconnect = (typeof opts.autoreconnect !== 'undefined' ? opts.autoreconnect : true);


  var db, server;

  var store = new Store(seneca);
  store.setErrorHandler(error);


  // seneca doesn't care about objects so we don't pass it the actual store object
  // instead we create a set of functions that implement the seneca store commands
  // the functions are mapped to actual store object functions
  var senecaStoreImpl = {};
  for (var p in Store.prototype) {
    if (Store.prototype.hasOwnProperty(p) && typeof (Store.prototype[p])==='function') {
      senecaStoreImpl[p] = makeFct(p);
    }
  }
  // returns a function that maps to a store object function with same name
  function makeFct(funcName) {
    return function() { Store.prototype[funcName].apply(store, arguments) }
  }


  // call seneca store init, passing in the set of seneca store commands implementations
  seneca.store.init(seneca, opts, senecaStoreImpl, function (err, tag, description) {
    if (err) return cb(err);

    // a seneca generated description object to be used for logging
    store.setDescription(description);


    // call configure to set up the db instance for the mongo store using the
    // mongo options in config file and connect to the server / open the database
    require('./configure').call(seneca, opts, function (err, dbInstance, dbServer) {
      if (err) {
        return seneca.fail({code: 'entity/configure', store: store.name, error: err}, cb)
      }

      // set/inject db instance into the store
      store.setDbInstance(dbInstance);

      db = dbInstance;
      server = dbServer;


      // all set, good to go
      cb(null, {name: store.name, tag: tag});

    })
  })


  function error(args, err, cb) {
    if (err) {
      seneca.log.debug('error: ' + err)

      // TODO: review
      // an auto-reconnect implementation, but is mongo auto reconnect not enough?
      // use only if mongo auto_reconnect does not behave as expected
      if ((opts.autoreconnect == 2) && ('ECONNREFUSED' == err.code || 'notConnected' == err.message || 'Error: no open connections' == err)) {
        if (minwait = opts.minwait) {

          reconnect(args, function(rerr) {
            if (rerr) {
              return seneca.fail({code: 'entity/error', store: store.name}, cb)
            }

            // attempt command again if reconnect successful
            Store.prototype[args.cmd].apply(store, [args, cb])
          })
        }
      }
      else {
        // fail immediately on error - default behaviour
        seneca.fail({code: 'entity/error', store: store.name}, cb)
      }

      return true
    }

    return false
  }


  var minwait;

  function reconnect(args, cb) {
    seneca.log.debug('attempting db reconnect')

    // close db first just in case
    db.close(function() {

    // attempt to connect
    db.open(function(err) {

      if (err) {
        seneca.log.debug('db reconnect (wait ' + opts.minwait + 'ms) failed: ' + err)
        minwait = 2 * minwait;
        if (minwait <= opts.maxwait) {
          setTimeout(function () {
            reconnect(args, cb)
          }, minwait)
        }
        else {
          cb(err)
        }
      }
      else {
        minwait = opts.minwait
        seneca.log.debug('reconnect ok')

        cb(null)
      }

    }) })
  }

}












