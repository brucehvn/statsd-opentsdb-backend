# StatsD OpenTSDB publisher backend

## Overview
This is a pluggable backend for [StatsD](https://github.com/etsy/statsd), which
publishes stats to OpenTSDB (http://opentsdb.net)

## Installation

    npm install statsd-opentsdb-backend

## Configuration
You have to give basic information about your OpenTSDB server to use
```
{
  opentsdbHosts: [{host: 'localhost', port: 4242}]
, opentsdbDeadHostRetry: 15
, opentsdbTagPrefix: '_t_'
, opentsdbTagValuePrefix: '_tv_'
, opentsdbDeadHostRetry: 15
}
```

## Tag support
This backend allows you to attach OpenTSDB tags to your metrics. To add a counter
called `gorets` and tag the data `foo=bar`, you'd write the following to statsd:

    gorets._t_foo._tv_bar:261|c

## Multiple opentsdb backend node support
This fork extends support for opentsdb by allowing you to configure an array of hash elements defining available hosts.
opentsdbHosts: [{host: 'localhost', port: 4242}]

Further configuration of this feature allows you to set the dead host retry period. Default value for this is 15
seconds. The configuration option for this setting is opentsdbDeadHostRetry.

## Dependencies
- none

## Development
- [Bugs](https://github.com/emurphy/statsd-opentsdb-backend/issues)

## Issues
If you want to contribute:

1. Clone your fork
2. Hack away
3. If you are adding new functionality, document it in the README
4. Push the branch up to GitHub
5. Send a pull request
