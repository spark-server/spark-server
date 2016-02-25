var cluster = require('cluster');
var vm = require('vm');
var fs = require('fs');
var Promise = require("bluebird");
var request = require('./request');
var _ = require("lodash");
var SparkContext = require('./sparkContext');
var config = require('./../config/config');
var commonFunctions = require('./commonFunctions');

// We use result parsing to avoid to have to implement respectively wrap the whole SparkScala/Java API
// Currently the focus is on possible return types of the DataFrame (see http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrame)
var parseFunctions = {
    getType: function (obj){
        return Object.prototype.toString.call(obj).slice(8, -1).replace(/nodeJava_/g, '').replace(/_/g, '.');
    },
    parse: function (obj){
        var nativeTypes = ["Boolean", "Number", "String", "Symbol", "Object"];
        var type = this.getType(obj);

        if (nativeTypes.indexOf(type) > -1) {
            return obj;
        } else if (type === "Array") {
            // Peek into array and determine type from the first entry
            var arrayType = parseFunctions.getType(obj[0]);
            return this.parsers[arrayType](obj);
        } else {
            if (this.parsers[type]) {
                return this.parsers[type](obj);
            } else {
                return "No parser found for type " + type;
            }
        }
    },
    parsers: {
        "org.apache.spark.sql.DataFrame": function(obj) {
            // Fastest way to get DataFrames as JSON objects!? Convert to RDD of JSON strings and then collect result
            return JSON.parse("["+obj.toJSON().collect().join(",")+"]");
        },
        "org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema": function(obj) {
            var rows = [];
            // Check if it's a single Row, if so, add to rows array, otherwise replace rows with obj array
            if (parseFunctions.getType(obj) !== "Array") {
                rows.push(obj);
            } else {
                rows = obj;
            }
            // Get schema from first row
            var schemaFields = JSON.parse(rows[0].schema().prettyJson()).fields,
                resultArray = [];

            // Iterate over all rows, and create row objects
            rows.forEach(function(row) {
                var i = 0,
                    resultObj = {};
                // Iterate over the row's fields
                schemaFields.forEach(function(field) {
                    resultObj[field.name] = row.get(i);
                    i++;
                });
                // Push to result array
                resultArray.push(resultObj);
            });

            return resultArray;
        },
        "String": function(obj) {
            return obj;
        },
        "java.util.Arrays.ArrayList": function(obj) {
            // Just transform to normal array
            return parseFunctions.parse(obj.toArray());
        },
        "org.apache.spark.sql.types.StructType": function(obj) {
            return JSON.parse(obj.prettyJson()).fields;
        },
        "scala.Tuple2": function(obj) {
            var tuple = obj.toString().split("),("),
                resultObj = {};
            tuple.forEach(function(entry) {
                var temp = entry.replace("(", "").replace(")", "").split(",");
                resultObj[temp[0]] = temp[1];
            });
            return resultObj;
        },
        "Null": function(obj) {
            return "Object of type 'Null' is not parseable!";
        },
        "Undefined": function(obj) {
            return "Object of type 'Undefined' is not parseable!";
        }
    }
};

var dataframeFunctions = {
    getRemoteJSON: function(url) {
        return new Promise(function(resolve, reject) {
            request({ url: url, json: true }).then(function(response) {
                var path = config.tmpCache+"/"+commonFunctions.uniqueId(20);
                fs.writeFile(path, JSON.stringify(response.body).replace("\\n", "").replace("\\r", ""), function(error) {
                    if (error) console.error("Error: " + JSON.stringify(error));
                    resolve(path);
                });
            }).catch(function(error) {
                console.error("Error: " + JSON.stringify(error));
                reject(error);
            });
        });
    },
    getFileById: function(fileId) {
        return config.fileCache + "/" + fileId;
    }
};

// Cache object for the Spark context instance
var contextCache = {};

if (cluster.isWorker) {

    // Store worker id
    contextCache.workerId = cluster.worker.id;

    // Handle nessages
    process.on('message', function(message) {

        //console.log("MSG received! "+ JSON.stringify(message));

        if (message.type) {
            switch (message.type.toLowerCase()) {

                case "create":

                    if (message.data && message.data.config) {

                        var contextConfig = message.data.config;

                        // Create context object instance with the provided settings
                        var sparkContext = new SparkContext(config);

                        // Create actual context
                        sparkContext.create(contextConfig, function(error, context) {

                            var sparkProperties = {};

                            // Get "real" Spark configuration from the context itself
                            context.sparkContext.getConf().getAll().forEach(function(propObj) {
                                sparkProperties[propObj["_1"]] = propObj["_2"];
                            });

                            // Replace request-defined Spark configuration
                            contextConfig.properties = sparkProperties;

                            // Store context in cache
                            contextCache = {
                                context: context,
                                configuration: contextConfig,
                                sessions: {}
                            };

                            // Load context methods
                            contextCache.context['getRemoteJSON'] = dataframeFunctions.getRemoteJSON;
                            contextCache.context['getFileById'] = dataframeFunctions.getFileById;
                            contextCache.context['request'] = request;

                            // Send message to the master process
                            process.send({ type: message.type, ok: true, data: { config: contextConfig } });

                        });

                    }

                    break;

                case "destroy_session":

                    // Remove all session-specific objects
                    if (contextCache.sessions && contextCache.sessions[message.sessionId]) {
                        delete contextCache.sessions[message.sessionId];
                    }

                    process.send({ type: message.type, ok: true });

                    break;

                case "destroy_context":

                    contextCache.context.destroy(function(error, result) {
                        // Send message to the master process
                        if (!error) {
                            process.send({ type: message.type, ok: result.ok });
                        } else {
                            process.send({ type: message.type, ok: result.ok, error: error });
                        }

                    });

                    break;

                case "execute":

                    // The general callback function for statement execution. This is called from within the transpiled code once everything is computed.
                    function callback(error) {
                        if (error) {

                            // Send error message
                            process.send({type: message.type, ok: false, result: null, error: error.message, listenerId: message.listenerId  });

                        } else {

                            var result = null;

                            // If something needs to be returned, continue
                            if (message.data.return) {

                                // Calculation of the return value
                                try {

                                    // Get unique id for the returnValue -> For parallel execution
                                    var returnValueId = "returnValue_" + commonFunctions.uniqueId(20);

                                    // Create return script
                                    var returnScript = new vm.Script(returnValueId + " = " + message.data.return);

                                    // Run virtual return script
                                    returnScript.runInContext(vmContext, config.vmContextConfiguration);

                                    // Assign result object
                                    result = vmContext[returnValueId];

                                    //console.log(parseFunctions.getType(result));

                                } catch (error) {
                                    // Send the error if some occurred
                                    process.send({type: message.type, ok: false, result: null, error: error.toString(), listenerId: message.listenerId  });
                                }

                                // Parsing of the return value
                                try {
                                    result = parseFunctions.parse(result);
                                } catch (parseError) {
                                    process.send({type: message.type, ok: false, result: result, error: parseError.toString(), listenerId: message.listenerId  });
                                }

                                // Cleanup returnValue in context
                                if (vmContext[returnValueId]) {
                                    delete vmContext[returnValueId];
                                }

                            }

                            // Send reply to master process
                            process.send({type: message.type, ok: true, result: result, listenerId: message.listenerId });

                            // Delete callback function from VM context
                            delete vmContext.callback;

                            // Merge context again after processing is finished
                            _.merge(contextCache.context, vmContext);

                        }
                    }

                    if (message.data) {

                        var code = null;

                        // Check if code parameter is supplied
                        if (message.data.code) {
                            if (Array.isArray(message.data.code)) {
                                code = message.data.code.join("; ");
                            } else {
                                code = message.data.code
                            }
                        }

                        // For parallelism, we need to first clone the context, and then merge it again after the execution finished
                        var vmContext = vm.createContext(_.cloneDeep(contextCache.context));

                        // Set actual callback configuration
                        vmContext.callback = callback;

                        try {

                            // Only run if code is provided
                            if (code) {

                                // Create virtual script from wrapped and transpiled code
                                var script = new vm.Script(commonFunctions.wrapAndTranspile(code));

                                // Run virtual script in session's vmContext
                                script.runInContext(vmContext, config.vmContextConfiguration);

                            } else {
                                vmContext.callback(null);
                            }

                        } catch (error) {
                            // Send the error if some occurred
                            process.send({type: message.type, ok: false, result: null, error: error.toString(), listenerId: message.listenerId  });
                        }


                    } else {
                        // Request can't be run
                        process.send({type: message.type, ok: false, result: null, error: "Please add some code to run!", listenerId: message.listenerId  });
                    }

                    break;

                default:

                    break;

            }
        }

    });
}