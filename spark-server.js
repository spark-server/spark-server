var cluster = require('cluster');
var express = require('express');
var bodyParser = require('body-parser');
var winston = require('winston');

cluster.setupMaster({
    exec: './lib/sparkContextWorker',
    silent: false
});

// Instantiate Express.js
var app = express();

// Load configuration
var config = require('./config/config');

// Configure logger
var logger = new (winston.Logger)({
    transports: [
        new (winston.transports.Console)({
            level: config.logLevel.toLowerCase(),
            timestamp: function() {
                return Date.now();
            },
            formatter: function(options) {
                // Return string will be passed to logger.
                return options.timestamp() + " " + options.level.toUpperCase() + " " + (undefined !== options.message ? options.message : "") +
                    (options.meta && Object.keys(options.meta).length ? '\n\t'+ JSON.stringify(options.meta) : "" );
            }
        })
    ]
});


if (cluster.isMaster) {

    var files = require('./routes/files')(config, logger);
    var sparkDistribution = require('./routes/sparkDistribution')(config, logger);
    var contexts = require('./routes/contexts')(config, logger, cluster);

    var apiVersionPrefix = '/v' + config.api.version;

    // General header settings
    app.set('x-powered-by', false);
    app.set('etag', false);

    // Use the bodyParser to parse JSON bodies
    app.use(bodyParser.json());

    // Routes
    app.use(apiVersionPrefix + '/files', files);
    app.use(apiVersionPrefix + '/contexts', contexts);

    // If enabled, server Spark distribution (for Mesos executors)
    if (config.sparkDistribution.enabled) {
        app.use(apiVersionPrefix + config.sparkDistribution.uri, sparkDistribution);
    }

    // Default route
    app.get(apiVersionPrefix + '/', function (req, res) {
        res.json({ message: "Welcome to Spark-Server!" });
    });

    // Health endpoint (e.g. for Marathon)
    app.get(apiVersionPrefix + '/health', function (req, res) {
        res.send("OK");
    });

    // API docs
    app.get(apiVersionPrefix + '/docs/api', function(req, res) {
        res.sendFile('docs/spark-server.html', {root: __dirname });
    });
    app.get(apiVersionPrefix + '/docs/api/raml', function(req, res) {
        res.sendFile('docs/spark-server.raml', {root: __dirname });
    });

    // Error handler
    app.use(function(err, req, res, next) {
        logger.error(err.stack);
        res.status(500).send({ message: err.message });
    });

    var server = app.listen(config.api.port, function () {
        var host = server.address().address;
        var port = server.address().port;

        logger.info('Spark-Server is listening at http://%s:%s', host, port);
    });

}

module.exports = app;