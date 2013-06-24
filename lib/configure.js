/* Copyright (c) 2010-2013 Richard Rodger, MIT License */
"use strict";

var _     = require('underscore');
var mongo = require('mongodb');

module.exports = function(spec, cb) {
  var seneca = this;

  // if connect not set, defer connection
  // TODO: expose connection action
  if( !_.isUndefined(spec.connect) && !spec.connect ) {
    return cb(null)
  }

  // the conf might be an object or a mongo:// string
  var conf = ('string' === typeof(spec)) ? parseSpec(spec) : spec

  conf.host = conf.host || conf.server
  conf.port = conf.port ? parseInt(conf.port, 10) : null
  conf.username = conf.username || conf.user
  conf.password = conf.password || conf.pass

  var serverOptions = _.extend({
    auto_reconnect: true
  }, { /* TODO: allow server options to be specified? */})

  var dbOptions = _.extend({
    native_parser: false,
    w: 1
  }, conf.options);

  var dbInstance, dbServer;

  // might have a simple server config or a replicaset config
  if (conf.replicaset) {
    dbServer = new mongo.ReplSetServers(
      _.map(conf.replicaset.servers, function(serverConfig) {
        return new mongo.Server(
          serverConfig.host || serverConfig.server,
          serverConfig.port || mongo.Connection.DEFAULT_PORT,
          serverOptions)
      })
    )
  }
  else {
    dbServer = new mongo.Server(
        conf.host || conf.server,
        conf.port || mongo.Connection.DEFAULT_PORT,
        serverOptions);
  }

  dbInstance = new mongo.Db(conf.name, dbServer, dbOptions);

  // open database / connect to server
  dbInstance.open(function (err) {
    if (err) {
      return cb(err, dbInstance, dbServer)
    }

    // authenticate if a username/password combination was specified
    if (conf.username) {
      dbInstance.authenticate(conf.username, conf.password, function (err) {
        if (err) {
          // do not attempt reconnect on auth error
          return cb(err, dbInstance, dbServer)
        }

        seneca.log.debug('init', 'db open and authed for ' + conf.username, dbOptions);
        cb(null, dbInstance, dbServer)
      })
    }
    else {
      seneca.log.debug('init', 'db open', dbOptions);
      cb(null, dbInstance, dbServer)
    }
  });
}

// parse a mongo connection string of form
// mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]
function parseSpec(spec) {

  var urlM = /^mongo:\/\/((.*?):(.*?)@)?(.*?)(:?(\d+))?\/(.*?)$/.exec(spec);

  return {
    name: urlM[7],
    port: urlM[6],
    server: urlM[4],
    username: urlM[2],
    password: urlM[3]
  }
}