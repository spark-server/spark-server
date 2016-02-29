'use strict';
var express = require('express');
var Tail = require('tail').Tail;
var router = express.Router();
var commonFunctions = require('../lib/commonFunctions');

function fillPorts(startPort, endPort) {
    for (var i=startPort; i <= endPort; i++) {
        availablePorts.push(i);
    }
}

function getAvailablePort() {

    // Get an index of the availablePorts array
    var indexInArray = Math.floor(Math.random() * (availablePorts.length-1));
    return availablePorts.splice(indexInArray, 1)[0];

}

function configurePorts(properties) {

    var portProperties = ["spark.blockManager.port", "spark.broadcast.port", "spark.driver.port", "spark.fileserver.port", "spark.ui.port"], //"spark.executor.port",
        usedPorts = [];

    portProperties.forEach(function(portProperty) {
        var port = getAvailablePort();
        properties[portProperty] = port.toString();
        usedPorts.push(port);
    });

    return { properties: properties, usedPorts: usedPorts };

}

// Used to store the contexts
var contexts = {};

// Available ports
var availablePorts = [];

var messageListeners = {};

module.exports = function(config, logger, cluster) {

    var ipAddress = commonFunctions.getIPAddress(config.spark.bindInterface);

    // Fill available ports
    fillPorts(config.spark.portRange.start, config.spark.portRange.end);

    // List all JARs
    router.get('/', function (req, res) {
        res.json({contexts: Object.keys(contexts)});
    });

    // Get specific Context
    router.get('/:contextName', function (req, res) {

        var contextName = req.params.contextName;

        if (contextName) {
            if (contexts[contextName]) {

                res.json({
                    context: contextName,
                    configuration: contexts[contextName].configuration,
                    sparkWebUi: "http://" + (contexts[contextName].configuration.properties["spark.driver.host"] ? contexts[contextName].configuration.properties["spark.driver.host"] : "localhost") + ":" + (contexts[contextName].configuration.properties["spark.ui.port"] ? contexts[contextName].configuration.properties["spark.ui.port"] : "4040"),
                    sessions: Object.keys(contexts[contextName].sessions).length
                });

            } else {
                res.json({ message: "Context " + contextName + " was not found!" });
            }
        } else {
            res.json({ message: "No Context provided!" });
        }

    });

    // Create Context
    router.post('/:contextName', function (req, res) {

        var contextName = req.params.contextName;
        var contextConfig = req.body.config || config.spark.standardContextConfig;

        // Add ports property to config object
        contextConfig.ports = [];

        // Ensure we have properties for the configuration
        if (!contextConfig.properties) {
            contextConfig.properties = {};
        }

        // If Spark-Server shall also serve the Spark distribution (for running on Mesos clusters)
        if (config.sparkDistribution.enabled) {

            // Extract file name from absolute path
            var pathArray = config.sparkDistribution.path.split("/");
            var fileName = pathArray[pathArray.length-1];

            // Set necessary spark.executor.uri property (URL!)
            contextConfig.properties["spark.executor.uri"] = "http://" + ipAddress + ":" + config.apiPort + config.sparkDistribution.uri + "/" + fileName;

        }

        // Set driver host ip address
        contextConfig.properties["spark.driver.host"] = ipAddress;

        // Set application name if provided, otherwise use contextName
        if (!contextConfig.applicationName) {
            contextConfig.applicationName = contextName;
        }

        // Create context
        if (contextName) {
            // Check if context with the contextName already exists, if not -> create
            if (!contexts[contextName]) {

                var timeoutTriggered = false;

                // Set port configuration (per context)
                var p = configurePorts(contextConfig.properties);
                contextConfig.properties = p.properties;
                contextConfig.ports = p.usedPorts;

                logger.info("Context creation: Available ports: " + availablePorts.length);

                // Store context in cache
                contexts[contextName] = {
                    contextWorker: cluster.fork(),
                    sessions: {},
                    configuration: contextConfig
                };

                // Add handler work context worker crashes/exits, so that the list of contexts is updated and the context objects are cleared.
                contexts[contextName].contextWorker.on('exit', function(code, signal) {

                    if( signal ) {
                        logger.warn("Context worker with id " + contexts[contextName].contextWorker.id + " of context " + contextName + " was killed by signal: "+signal);
                    } else if( code !== 0 ) {
                        logger.warn("Context worker with id " + contexts[contextName].contextWorker.id + " of context " + contextName + " exited with error code: "+code);
                    } else {
                        logger.warn("Context worker with id " + contexts[contextName].contextWorker.id + " of context " + contextName + " was shut down");
                    }

                    // Free the used ports
                    availablePorts = availablePorts.concat(contexts[contextName].configuration.ports);

                    logger.warn("Context " + contextName + " destroyed: Available ports: " + availablePorts.length);

                    // Delete context object if worker exited
                    delete contexts[contextName];

                });

                // Send creation message
                contexts[contextName].contextWorker.send({ type: "create", data: { config: contextConfig} });

                // Initiate a timeout handler, if context creation is not successful within timeout
                var contextTimeout = setTimeout(function() {
                    timeoutTriggered = true;
                    res.status(500).json({ ok: false, message: "Context " + contextName + " couldn't be created within timeout!" });
                }, config.spark.createContextTimeout);

                // Inform when the context work is online
                contexts[contextName].contextWorker.on('online', function() {
                    // Context worker is online
                    logger.info("Context worker with id " + contexts[contextName].contextWorker.id + " is online!")

                });

                // Get unique listenerId. We need this for the parallel/async comminucation with our worker context process
                var listenerId = commonFunctions.uniqueId(20);

                // Request-unique messageListener
                messageListeners[listenerId] = function(message) {

                    if (message.type === "create" && message.ok) {

                        // Clear timeout
                        clearTimeout(contextTimeout);

                        logger.info("Context worker with id " + contexts[contextName].contextWorker.id + " has created context '" + contextName + "'");

                        // Store logFile reference -> will be used by the
                        contexts[contextName].logFile = new Tail(message.data.logPath, /[\r]{0,1}\n/, {}, true);

                        // Create log array
                        contexts[contextName].logs = [];

                        // Event for new lines
                        contexts[contextName].logFile.on("line", function(data) {
                            // Push lines to log array
                            contexts[contextName].logs.push(data.replace(/\t/g, ""));
                        });

                        // Event for watch errors
                        contexts[contextName].logFile.on("error", function(error) {
                            console.log('ERROR: ', error);
                        });

                        logger.info("Context worker with id " + contexts[contextName].contextWorker.id + " has logPath " + message.data.logPath);


                        // Only send response if timeout was not yet triggered
                        if (!timeoutTriggered) {
                            // Send response, everything is ok
                            res.status(201).json({
                                context: contextName,
                                configuration: message.data.config,
                                sparkWebUi: "http://" + (contextConfig.properties["spark.driver.host"] ? contextConfig.properties["spark.driver.host"] : ipAddress) + ":" + (contextConfig.properties["spark.ui.port"] ? contextConfig.properties["spark.ui.port"] : "4040"),
                                sessions: Object.keys(contexts[contextName].sessions).length
                            });
                        }

                    }
                    // Remove listener
                    contexts[contextName].contextWorker.removeListener("message", messageListeners[listenerId]);
                };

                // Message handling
                contexts[contextName].contextWorker.on('message', messageListeners[listenerId]);

            } else {
                res.json({ context: contextName, message: "Context " + contextName + " already exists!" });
            }
        } else {
            res.status(500).json({ message: "No context name specified!"});
        }

    });

    // Delete a context
    router.delete('/:contextName', function (req, res) {

        var contextName = req.params.contextName;

        if (contextName) {
            if (contexts[contextName]) {

                // Send message for context worker's Spark context shutdown
                contexts[contextName].contextWorker.send({ type: "destroy_context" });

                // Kill context worker process
                contexts[contextName].contextWorker.kill();

                // Unwatch logFile
                contexts[contextName].logFile.unwatch();

                // Send response
                res.json({ context: contextName, ok: true});

            } else {
                res.status(404).json({ message: "Context " + contextName + " was not found!" });
            }
        } else {
            res.status(500).json({ message: "No Context provided!" });
        }

    });

    router.get('/:contextName/logs', function (req, res) {

        var contextName = req.params.contextName;
        var tailStart = (req.query.tail && parseInt(req.query.tail) > 0 ? (req.query.tail <= contexts[contextName].logs.length ? contexts[contextName].logs.length-req.query.tail : 0) : null);

        if (contextName) {
            if (contexts[contextName]) {
                if (tailStart) {
                    res.json({ context: contextName, logs: contexts[contextName].logs.slice(tailStart, contexts[contextName].logs.length) });
                } else {
                    res.json({ context: contextName, logs: contexts[contextName].logs });
                }
            } else {
                res.status(404).json({ message: "Context " + contextName + " was not found!" });
            }
        } else {
            res.status(500).json({ message: "No Context provided!" });
        }

    });

    router.delete('/:contextName/logs', function (req, res) {

        var contextName = req.params.contextName;

        if (contextName) {
            if (contexts[contextName]) {

                // Truncate logs array
                contexts[contextName].logs.length = 0;

                res.json({ context: contextName, logs: contexts[contextName].logs });

            } else {
                res.status(404).json({ message: "Context " + contextName + " was not found!" });
            }
        } else {
            res.status(500).json({ message: "No Context provided!" });
        }

    });

    router.get('/:contextName/sessions', function (req, res) {

        var contextName = req.params.contextName;

        if (contextName) {
            if (contexts[contextName]) {

                // Transfor session object to session array (nicer!) for displaying
                var sessionArray = [];
                Object.keys(contexts[contextName].sessions).forEach(function(sessionId){
                    sessionArray.push(contexts[contextName].sessions[sessionId]);
                });

                res.json({
                    context: contextName,
                    sessions: sessionArray,
                    sessionsCount: Object.keys(contexts[contextName].sessions).length
                });

            } else {
                res.status(404).json({ message: "Context " + contextName + " was not found!" });
            }
        } else {
            res.status(500).json({ message: "No Context provided!" });
        }

    });

    router.post('/:contextName/sessions', function (req, res) {

        var contextName = req.params.contextName;

        if (contextName) {
            if (contexts[contextName]) {

                var sessionId = commonFunctions.uniqueId(20);

                // Create session for existing context
                if (!contexts[contextName].sessions[sessionId]) {
                    contexts[contextName].sessions[sessionId] = {
                        id: sessionId,
                        startedTimestamp: new Date().getTime(),
                        type: "spark",
                        status: "IDLE",
                        currentlyRunning: 0,
                        statements: []
                    };
                }

                res.status(201).json({
                    context: contextName,
                    session: contexts[contextName].sessions[sessionId]
                });

            } else {
                res.status(404).json({ message: "Context " + contextName + " was not found!" });
            }
        } else {
            res.status(500).json({ message: "No Context provided!" });
        }

    });

    router.get('/:contextName/sessions/:sessionId', function (req, res) {
        var contextName = req.params.contextName;
        var sessionId = req.params.sessionId;

        if (contextName) {
            if (sessionId) {
                if (contexts[contextName] && contexts[contextName].sessions[sessionId]) {

                    res.json({
                        context: contextName,
                        session: contexts[contextName].sessions[sessionId]
                    });

                } else {
                    res.status(404).json({ message: "SessionId " + sessionId + " was not found for context " + contextName + "!" });
                }
            } else {
                res.status(500).json({ message: "No SessionId provided!" });
            }
        } else {
            res.status(500).json({ message: "No Context provided!" });
        }
    });

    router.delete('/:contextName/sessions/:sessionId', function (req, res) {

        var contextName = req.params.contextName;
        var sessionId = req.params.sessionId;

        if (contextName) {
            if (sessionId) {
                if (contexts[contextName] && contexts[contextName].sessions[sessionId]) {

                    // Send message for context worker's Spark session shutdown
                    contexts[contextName].contextWorker.send({ type: "destroy_session", sessionId: sessionId });

                    // Get unique listenerId. We need this for the parallel/async comminucation with our worker context process
                    var listenerId = commonFunctions.uniqueId(20);

                    // One-time event listener
                    messageListeners[listenerId] = function(message) {
                        if (message && message.type === "destroy_session") {
                            // Remove from context's sessions
                            delete contexts[contextName].sessions[sessionId];

                            res.json({
                                context: contextName,
                                sessionId: sessionId
                            });
                        }
                        // Remove listener
                        contexts[contextName].contextWorker.removeListener("message", messageListeners[listenerId]);
                    };

                    // Wait for result to be returned (via message type == destroySession)
                    contexts[contextName].contextWorker.on("message", messageListeners[listenerId]);

                } else {
                    res.status(404).json({ message: "SessionId " + sessionId + " was not found for context " + contextName + "!" });
                }
            } else {
                res.status(404).json({ message: "No SessionId provided!" });
            }

        } else {
            res.status(404).json({ message: "No Context provided!" });
        }

    });

    router.post('/:contextName/sessions/:sessionId/statements', function (req, res) {

        var contextName = req.params.contextName;
        var sessionId = req.params.sessionId;
        var executionObj = req.body;

        // Add sessionId
        executionObj.sessionId = sessionId;

        if (contextName) {
            if (sessionId) {
                if (contexts[contextName] && contexts[contextName].sessions[sessionId]) {
                    if (executionObj) {

                        // Store for replay
                        var replayStatements = [],
                            statementCount = contexts[contextName].sessions[sessionId].statements.length;

                        if (executionObj.code && Array.isArray(executionObj.code)) {
                            // Create a new statement object
                            executionObj.code.forEach(function(statement){
                                replayStatements.push({
                                    executionTimestamp: new Date().getTime(),
                                    statementId: statementCount,
                                    statement:statement
                                });
                                statementCount++;
                            });
                        }

                        // Get unique listenerId. We need this for the parallel/async comminucation with our worker context process
                        var listenerId = commonFunctions.uniqueId(20);

                        // Store statements
                        contexts[contextName].sessions[sessionId].statements = contexts[contextName].sessions[sessionId].statements.concat(replayStatements);

                        // Send message for context worker for code execution
                        contexts[contextName].contextWorker.send({ type: "execute", data: executionObj, listenerId: listenerId });

                        // Set session status to RUNNING
                        contexts[contextName].sessions[sessionId].status = "RUNNING";

                        // Increase currently running statements counter
                        contexts[contextName].sessions[sessionId].currentlyRunning++;

                        // Create unique message listener for this request
                        messageListeners[listenerId] = function(message) {
                            // Make sure that it's an execute message (=response) and the listenerId matches the current request
                            if (message && message.type === "execute" && message.listenerId === listenerId) {
                                // Decrease running statements counter
                                contexts[contextName].sessions[sessionId].currentlyRunning--;
                                // Set session status to IDLE if no other running statements
                                if (contexts[contextName].sessions[sessionId].currentlyRunning === 0) {
                                    contexts[contextName].sessions[sessionId].status = "IDLE";
                                }
                                // Return response with result
                                if (message.ok) {
                                    res.json({
                                        context: contextName,
                                        sessionId: sessionId,
                                        result: message.result
                                    });
                                } else {
                                    res.status(500).json({
                                        context: contextName,
                                        sessionId: sessionId,
                                        error: message.error.toString()
                                    });
                                }

                                // Remove listener
                                contexts[contextName].contextWorker.removeListener("message", messageListeners[listenerId]);

                            }

                        };

                        // Wait for result to be returned (via message type == execute)
                        contexts[contextName].contextWorker.on("message", messageListeners[listenerId]);

                    } else {
                        res.status(500).json({ message: "No code to execute provided!" });
                    }

                } else {
                    res.status(404).json({ message: "SessionId " + sessionId + " was not found for context " + contextName + "!" });
                }
            } else {
                res.status(500).json({ message: "No SessionId provided!" });
            }
        } else {
            res.status(500).json({ message: "No Context provided!" });
        }

    });

    router.get('/:contextName/sessions/:sessionId/dataframes/:dataframeName/:operation', function (req, res) {
        var contextName = req.params.contextName;
        var sessionId = req.params.sessionId;
        var dataframeName = req.params.dataframeName;
        var operation = req.params.operation ? req.params.operation.toLowerCase() : null;
        var cutOffThreshold = req.query.cutOffThreshold ? req.query.cutOffThreshold : config.spark.statistics.cutOffThreshold;

        if (contextName) {
            if (sessionId) {
                if (dataframeName) {
                    if (operation && Object.getOwnPropertyNames(config.spark.dataframeOperations).indexOf(operation) > -1) {
                        if (contexts[contextName] && contexts[contextName].sessions[sessionId]) {

                            // Response sent?
                            var responseSent = false;

                            // Code to execute
                            var code = "";

                            // The operation "data" requires additional options, and "stats" can have a "cutOffThreshold" paramenter as well,
                            // which needs to be handled. The other operations don't have any parameters
                            if (operation === "data") {
                                if (req.query.sortFields && req.query.start && req.query.limit) {
                                    var sortFieldsArray = req.query.sortFields.indexOf(",") > -1 ? req.query.sortFields.split(",") : [req.query.sortFields.toString()];
                                    code = config.spark.dataframeOperations[operation] + "(" + dataframeName + ", ['" + sortFieldsArray.join("','") + "']," + req.query.start + ", " + req.query.limit + ");";
                                } else {
                                    res.status(500).json({ ok: false, message: "Operation /data on a dataframe requires the mandatory parameters sortFields, start and limit!" });
                                    responseSent = true;
                                }
                            } else if (operation === "stats") {
                                code = config.spark.dataframeOperations[operation] + "(" + dataframeName + ", " + cutOffThreshold + ", true);";
                            } else {
                                code = config.spark.dataframeOperations[operation] + "(" + dataframeName + ");";
                            }

                            // Get unique listenerId. We need this for the parallel/async communication with our worker context process
                            var listenerId = commonFunctions.uniqueId(20);

                            // Create statement object (for session replay)
                            var statement = {
                                executionTimestamp: new Date().getTime(),
                                statementId: contexts[contextName].sessions[sessionId].statements.length,
                                statement: code
                            };

                            // The execution object which is passed to the context worker
                            var executionObj = {
                                "return": code
                            };

                            // Store statement
                            contexts[contextName].sessions[sessionId].statements.push(statement);

                            // Send message for context worker for code execution
                            contexts[contextName].contextWorker.send({ type: "execute", data: executionObj, listenerId: listenerId });

                            // Set session status to RUNNING
                            contexts[contextName].sessions[sessionId].status = "RUNNING";

                            // Create unique message listener for this request
                            messageListeners[listenerId] = function(message) {
                                if (message && message.type === "execute" && message.listenerId === listenerId && !responseSent) {
                                    // Set session status to RUNNING
                                    contexts[contextName].sessions[sessionId].status = "IDLE";
                                    // Return response with result
                                    if (message.ok) {
                                        res.json({
                                            context: contextName,
                                            sessionId: sessionId,
                                            dataframe: dataframeName,
                                            result: message.result
                                        });
                                    } else {
                                        res.status(500).json({
                                            context: contextName,
                                            sessionId: sessionId,
                                            dataframe: dataframeName,
                                            error: message.error.toString()
                                        });
                                    }

                                    // Remove listener
                                    contexts[contextName].contextWorker.removeListener("message", messageListeners[listenerId]);

                                }

                            };

                            // Wait for result to be returned (via message type == execute)
                            contexts[contextName].contextWorker.on("message", messageListeners[listenerId]);


                        } else {
                            res.status(404).json({ ok: false, message: "SessionId " + sessionId + " was not found for context " + contextName + "!" });
                        }
                    } else {
                        res.status(500).json({ ok: false, message: "No operation provided, or operation does not match the allowed operations!" });
                    }
                } else {
                    res.status(500).json({ ok: false, message: "No DataframeName provided!" });
                }
            } else {
                res.status(500).json({ ok: false, message: "No SessionId provided!" });
            }
        } else {
            res.status(500).json({ ok: false, message: "No Context provided!" });
        }
    });

    return router;

};