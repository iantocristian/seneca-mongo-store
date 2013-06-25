/* Copyright (c) 2010-2013 Richard Rodger, MIT License */
"use strict";

var _     = require('underscore');
var mongo = require('mongodb')

// TODO: review, using mongodb private parser
var mongourlparser = require('mongodb/lib/mongodb/connection/url_parser');


module.exports = function(opts, cb) {
  var seneca = this;

  // if connect not set, defer connection
  // TODO: expose connection action, review, do we need this
  if( !_.isUndefined(opts.connect) && !opts.connect ) {
    return cb(null)
  };

  var conf = buildConfig(opts);

  var dbServer;
  if (conf.replicaSetSpecified) {
    dbServer = new mongo.ReplSet(
      _.map(conf.servers, function(serverConfig) {
        return new mongo.Server(
          serverConfig.host || serverConfig.server,
          serverConfig.port || mongo.Connection.DEFAULT_PORT,
          conf.server_options)
      })
    ),
    conf.rs_options
  }
  else {
    dbServer = new mongo.Server(
      conf.host || conf.server,
      conf.port || mongo.Connection.DEFAULT_PORT,
      conf.server_options
    );
  }

  var dbInstance = new mongo.Db(conf.name, dbServer, conf.db_options);

  // open database / connect to server
  dbInstance.open(function (err) {
    if (err) {
      return cb(err, dbInstance, dbServer)
    }

    // authenticate if a username/password combination was specified
    if (conf.auth) {
      dbInstance.authenticate(conf.auth.user, conf.auth.password, function (err) {
        if (err) {
          // do not attempt reconnect on auth error
          return cb(err, dbInstance, dbServer)
        }

        seneca.log.debug('init', 'db open and authed for ' + conf.username, conf.db_options);
        cb(null, dbInstance, dbServer)
      })
    }
    else {
      seneca.log.debug('init', 'db open', conf.db_options);
      cb(null, dbInstance, dbServer)
    }
  });
}


function buildConfig(opts) {

  var conf = {
    replicaSetSpecified: false
  };

  // default options
  conf.server_options = { auto_reconnect: true};
  conf.db_options = { native_parser: false, w: 1 };
  conf.rs_options = {};

  // if a connection string was specified, parse and use
  if (opts.url) {
    var object = mongourlparser.parse(opts.url);
    conf.name = object.dbName;

    if (object.servers.length > 1) {
      conf.replicaSetSpecified = true;
      conf.servers = object.servers;
    }
    else if (object.servers.length == 1) {
      conf.host = object.servers[0].host;
      conf.port = object.servers[0].port;
    }

    if (object.auth) {
      conf.auth = object.auth;
    }

    // apply connection string options on top of the default ones
    _.extend(conf.server_options, object.server_options);
    _.extend(conf.db_options, object.db_options);
    _.extend(conf.rs_options, object.rs_options);
  }

  // apply opts
  conf.name = opts.name || conf.name;
  conf.host = opts.host || opts.server || conf.host;
  conf.port = opts.port ? parseInt(opts.port, 10) : conf.port;

  if (opts.username) {
    conf.auth = {
      user: opts.username || opts.user,
      password: opts.password || opts.pass
    }
  }

  // mongodb driver auto-reconnect is enabled by default, set autoreconnect to false or 0 to disable
  // when autoreconnect is 2 both driver auto-reconnect and custom reconnect implementation are enabled
  if (typeof opts.autoreconnect !== 'undefined') {
   conf.server_options.auto_reconnect = (opts.autoreconnect === true || opts.autoreconnect > 0)
  }
  _.extend(conf.server_options, { /* TODO: allow server options to be specified? */ })
  _.extend(conf.db_options, opts.options);

  // apply opts replicaset
  if (opts.replicaset) {
    conf.replicaSetSpecified = true;
    _.extend(conf.rs_options, opts.replicaset.options);
    if (opts.replicaset.servers) {
      conf.servers = opts.replicaset.servers;
    }
  }

  return conf;
}


// parseSpec no longer used, replaced with mongodb url_parser
//
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