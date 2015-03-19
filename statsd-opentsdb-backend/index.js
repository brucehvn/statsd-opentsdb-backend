/*
 * Flush stats to OpenTSDB (http://opentsdb.net/).
 *
 * To enable this backend, include 'opentsdb' in the backends
 * configuration array:
 *
 *   backends: ['opentsdb']
 *
 * This backend supports the following config options:
 *
 *   opentsdbHost: Hostname of opentsdb server.
 *   opentsdbPort: Port to contact opentsdb server at.
 */

var net = require('net'),
   util = require('util');

var debug;
var flushInterval;

var opentsdbHosts;
var opentsdbSelectedHost = null;
var opentsdbDeadHostRetry;

var opentsdbTagPrefix;
var opentsdbTagValuePrefix;

// prefix configuration
var globalPrefix;
var prefixPersecond;
var prefixCounter;
var prefixTimer;
var prefixGauge;
var prefixSet;

// set up namespaces
var legacyNamespace = true;
var globalNamespace  = [];
var counterNamespace = [];
var timerNamespace   = [];
var gaugesNamespace  = [];
var setsNamespace     = [];

var statsdLogger;

var opentsdbStats = {};

function get_timestamp() {
  return Math.round(new Date().getTime() / 1000);
}

var select_host = function opentsdb_select_host() {
  opentsdbSelectedHost = null;

  if (opentsdbHosts.length > 0) {
    for (var i in opentsdbHosts) {
      // If no deadTime value has been defined we assume host is alive and return it
      if (typeof opentsdbHosts[i].deadTime === 'undefined') {
        if (debug) { util.log('Selected ' + opentsdbHosts[i] + ' as assumed live host.\n'); }
        opentsdbSelectedHost = opentsdbHosts[i];
      } else if (get_timestamp() - opentsdbHosts[i].deadTime >= opentsdbDeadHostRetry) {
        if (debug) { util.log('Retrying ' + opentsdbHosts[i] + ' as current live host.\n'); }
        delete opentsdbHosts[i].deadTime; // Remove expired deadTime
        opentsdbSelectedHost = opentsdbHosts[i];
      }
    }
  }

  if (opentsdbSelectedHost == null) util.log('Failed to find a opentsdb host that was not flagged as dead, will attempt again shortly.\n');
  return opentsdbSelectedHost;
}

var mark_dead_host = function opentsdb_mark_dead_host(deadHost) {
  opentsdbSelectedHost = null;

  if (opentsdbHosts.length > 0) {
    for (var i in opentsdbHosts) {
      if (opentsdbHosts[i].host == deadHost.host) {
        var ts = get_timestamp();
        opentsdbHosts[i].deadTime = ts;

        if (debug) {
          util.log(deadHost.host + ' marked dead. Host will be re-checked for availability after ' + opentsdbDeadHostRetry + ' seconds.\n');
        }
      }
    }
  }
}

var post_stats = function opentsdb_post_stats(statString) {
  var last_flush = opentsdbStats.last_flush || 0;
  var last_exception = opentsdbStats.last_exception || 0;

  opentsdbHost = opentsdbSelectedHost || select_host();

  if (opentsdbHost) {
    try {
      var opentsdb = net.createConnection(opentsdbHost);
      opentsdb.addListener('error', function(connectionException){
        mark_dead_host(opentsdbHost);

        if (debug) {
          util.log(connectionException);
        }
      });
      opentsdb.on('connect', function() {
        var ts = get_timestamp();
        var namespace = globalNamespace.concat('statsd');
        statString += 'put ' + namespace.join(".") + '.opentsdbStats.last_exception ' + last_exception + ' ' + ts + "\n";
        statString += 'put ' + namespace.join(".") + '.opentsdbStats.last_flush ' + last_flush + ' ' + ts + "\n";
		    if (debug) {
			    util.log(statString)
		    }
        this.write(statString);
        this.end();
        opentsdbStats.last_flush = get_timestamp();
      });
    } catch(e){
      if (debug) {
        util.log(e);
      }
      opentsdbStats.last_exception = get_timestamp();
    }
  }
}

// Returns a list of "tagname=tagvalue" strings from the given metric name.
function parse_tags(metric_name) {
  if (debug) { statsdLogger.log('parseTags, metric_name = ' + metric_name, 'DEBUG'); }
  var tagsArray = metric_name.split("." + opentsdbTagPrefix);
  var tags = [];
  
  // remove the metric name from the array
  tagsArray.shift();
  if (debug) { statsdLogger.log('tagsArray = ' + tagsArray.toString(), 'DEBUG'); }
  var numTags = tagsArray.length;
  
  for (var xctr = 0; xctr < numTags; xctr++) {
    var rawTag = tagsArray[xctr];
    
    // first see if we have something in the format _t_tagname_tv_tagvalue
    var tagParts = rawTag.split("." + opentsdbTagValuePrefix);
    if (debug) { statsdLogger.log('tagParts = ' + tagParts.toString(), 'DEBUG'); }
    if (tagParts.length < 2) {
      // try the original format _t_tagname.tagvalue
      tagParts = rawTag.split(".");
      if (tagParts.length != 2) {
        if (debug) { statsdLogger.log('Bad tag format: ' + metric_name, 'DEBUG'); }
        continue;
      }
    }
    
    var current_tag_name = tagParts[0] + "=" + tagParts[1];
    tags.push(current_tag_name);
  }
  
  return(tags);
}

// Strips out all tag information from the given metric name
function strip_tags(metric_name) {
  var tagsArray = metric_name.split("." + opentsdbTagPrefix);
  var retval = "";
  
  if (tagsArray.length > 0) {
    retval = tagsArray.shift();
  }
  
  return(retval);
}


var flush_stats = function opentsdb_flush(ts, metrics) {
  var suffix = " " + postSuffix;
  var starttime = Date.now();
  var statString = '';
  var numStats = 0;
  var key;
  var timer_data_key;
  var counters = metrics.counters;
  var gauges = metrics.gauges;
  var timers = metrics.timers;
  var sets = metrics.sets;
  var timer_data = metrics.timer_data;
  var statsd_metrics = metrics.statsd_metrics;

  for (key in counters) {
    var tags = parse_tags(key);
    var stripped_key = strip_tags(key)

    var namespace = counterNamespace.concat(stripped_key);
    var value = counters[key];

    if (legacyNamespace === true) {
      statString += 'put stats_counts.' + stripped_key + ' ' + ts + ' ' + value + ' ' + tags.join(' ') + suffix;
    } else {
      if (counterSuffix.length > 0 ) {
        namespace.concat(counterSuffix);
      }
      statString += 'put ' + namespace.join(".") + ' ' + ts + ' ' + value + ' ' + tags.join(' ') + suffix;
    }

    numStats += 1;
  }

  for (key in timer_data) {
    if (Object.keys(timer_data).length > 0) {
      for (timer_data_key in timer_data[key]) {
        var tags = parse_tags(key);
        var stripped_key = strip_tags(key)

        var namespace = timerNamespace.concat(stripped_key);
        var the_key = namespace.join(".");
        statString += 'put ' + the_key + '.' + timer_data_key + ' ' + ts + ' ' + timer_data[key][timer_data_key] + ' ' + tags.join(' ') + suffix;
      }

      numStats += 1;
    }
  }

  for (key in gauges) {
    var tags = parse_tags(key);
    var stripped_key = strip_tags(key)

    var namespace = gaugesNamespace.concat(stripped_key);
    statString += 'put ' + namespace.join(".") + ' ' + ts + ' ' + gauges[key] + ' ' + tags.join(' ') + suffix;
    numStats += 1;
  }

  for (key in sets) {
    var tags = parse_tags(key);
    var stripped_key = strip_tags(key)

    var namespace = setsNamespace.concat(stripped_key);
    statString += 'put ' + namespace.join(".") + '.count ' + ts + ' ' + sets[key].values().length + ' ' + tags.join(' ') + suffix;
    numStats += 1;
  }

  var namespace = globalNamespace.concat('statsd');
  if (legacyNamespace === true) {
    statString += 'put statsd.numStats ' + ts + ' ' + numStats + suffix;
    statString += 'put stats.statsd.opentsdbStats.calculationtime ' + ts + ' ' + (Date.now() - starttime) + suffix;
    for (key in statsd_metrics) {
      statString += 'put stats.statsd.' + key + ' ' + ts + ' ' + statsd_metrics[key] + suffix;
    }
  } else {
    statString += 'put ' + namespace.join(".") + '.numStats ' + ts + ' ' + numStats + suffix;
    statString += 'put ' + namespace.join(".") + '.opentsdbStats.calculationtime ' + ts + ' ' + (Date.now() - starttime) + suffix;
    for (key in statsd_metrics) {
      var the_key = namespace.concat(key);
      statString += 'put ' + the_key.join(".") + ' ' + ts + ' ' + statsd_metrics[key] + suffix;
    }
  }

  post_stats(statString);
};

var backend_status = function opentsdb_status(writeCb) {
  for (stat in opentsdbStats) {
    writeCb(null, 'opentsdb', stat, opentsdbStats[stat]);
  }
};

exports.init = function opentsdb_init(startup_time, config, events, logger) {
  debug = config.debug;
  opentsdbHosts = config.opentsdbHosts;
  opentsdbDeadHostRetry = config.opentsdbDeadHostRetry || 15;
  opentsdbSelectedHost = null;
  statsdLogger = logger;
  opentsdbTagPrefix = config.opentsdbTagPrefix;
  opentsdbTagValuePrefix = config.opentsdbTagValuePrefix;
  config.opentsdb = config.opentsdb || {};
  globalPrefix    = config.opentsdb.globalPrefix;
  prefixCounter   = config.opentsdb.prefixCounter;
  prefixTimer     = config.opentsdb.prefixTimer;
  prefixGauge     = config.opentsdb.prefixGauge;
  prefixSet       = config.opentsdb.prefixSet;
  legacyNamespace = config.opentsdb.legacyNamespace;
  postSuffix = config.opentsdb.postSuffix;
  counterSuffix = config.opentsdb.counterSuffix;

  // set defaults for prefixes
  globalPrefix  = globalPrefix !== undefined ? globalPrefix : "stats";
  prefixCounter = prefixCounter !== undefined ? prefixCounter : "counters";
  prefixTimer   = prefixTimer !== undefined ? prefixTimer : "timers";
  prefixGauge   = prefixGauge !== undefined ? prefixGauge : "gauges";
  prefixSet     = prefixSet !== undefined ? prefixSet : "sets";
  legacyNamespace = legacyNamespace !== undefined ? legacyNamespace : true;
  
  opentsdbTagPrefix = opentsdbTagPrefix !== undefined ? opentsdbTagPrefix : "_t_";
  opentsdbTagValuePrefix = opentsdbTagValuePrefix !== undefined ? opentsdbTagValuePrefix : "_tv_";
  postSuffix = postSuffix !== undefined ? postSuffix : "\n";
  counterSuffix = counterSuffix !== undefined ? counterSuffix : "";

  if (debug) { statsdLogger.log('opentsdbTagPrefix: ' + opentsdbTagPrefix + ", opentsdbTagValuePrefix: " + opentsdbTagValuePrefix, "DEBUG"); }
  if (debug) { statsdLogger.log('opentsdbDeadHostRetry: ' + opentsdbDeadHostRetry + ', opentsdbHosts: ' + opentsdbHosts, "DEBUG"); }
  
  if (legacyNamespace === false) {
    if (globalPrefix !== "") {
      globalNamespace.push(globalPrefix);
      counterNamespace.push(globalPrefix);
      timerNamespace.push(globalPrefix);
      gaugesNamespace.push(globalPrefix);
      setsNamespace.push(globalPrefix);
    }

    if (prefixCounter !== "") {
      counterNamespace.push(prefixCounter);
    }
    if (prefixTimer !== "") {
      timerNamespace.push(prefixTimer);
    }
    if (prefixGauge !== "") {
      gaugesNamespace.push(prefixGauge);
    }
    if (prefixSet !== "") {
      setsNamespace.push(prefixSet);
    }
  } else {
      globalNamespace = ['stats'];
      counterNamespace = ['stats'];
      timerNamespace = ['stats', 'timers'];
      gaugesNamespace = ['stats', 'gauges'];
      setsNamespace = ['stats', 'sets'];
  }

  opentsdbStats.last_flush = startup_time;
  opentsdbStats.last_exception = startup_time;

  flushInterval = config.flushInterval;

  events.on('flush', flush_stats);
  events.on('status', backend_status);

  return true;
};
