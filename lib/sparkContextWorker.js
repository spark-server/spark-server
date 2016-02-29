var cluster = require('cluster');
var vm = require('vm');
var fs = require('fs');
var path = require('path');
var Promise = require("bluebird");
var request = require('./request');
var _ = require("lodash");
var SparkContext = require('./sparkContext');
var config = require('./../config/config');
var commonFunctions = require('./commonFunctions');

// Cache object for the Spark context instance
var contextCache = {};

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
    },
    getSchema: function(dataframe) {
        return dataframe.schema();
    },
    getData: function(dataframe, sortFieldArray, startIndex, limit) {

        var jsColumnArray = [];

        if (Array.isArray(sortFieldArray)) {
            sortFieldArray.forEach(function(sortField) {
                jsColumnArray.push(dataframe.col(sortField));
            });
        }

        var columnArray = this.java.newArray('org.apache.spark.sql.Column', jsColumnArray);

        // Create "windowed" DataFrame by using limit() and execpt() methods
        // Currently there seems to be no better/faster way to implement this with the Spark DataFrame APIs IMHO
        var df = dataframe.sort(columnArray),
            df_count = df.count(),
            df1_limit = df.limit(startIndex),
            df2_limit = df.except(df1_limit).sort(columnArray),
            results = df2_limit.limit(limit).sort(columnArray).toJSON().collect();

        return {
            data: JSON.parse("["+results.join(",")+"]"),
            overallCount: JSON.parse(df_count)
        };

    },
    getStatistics: function(dataframe, cutOffThreshold, respectThreshold) {
        var self = this;
        var schema = JSON.parse(dataframe.schema().prettyJson());
        var stringFields = [],
            nonStringFields = [],
            statistics = {};

        // Iterate over schema field array
        schema.fields.forEach(function(field){
            // Store field datatypes in statistics map
            statistics[field.name] = {
                dataType: field.type
            };
            // Determine string/non-string fields -> Treated differently!
            if (field.type === "string") {
                stringFields.push(field.name);
            } else {
                nonStringFields.push(field.name);
            }
        });

        // Create non-string fields statistics
        var nonStringStatsResults = dataframe.describe(nonStringFields).toJSON().collect();

        nonStringStatsResults.forEach(function(typeResultString) {
            var typeResult = JSON.parse(typeResultString);
            var field = typeResult.summary;
            var properties = Object.getOwnPropertyNames(typeResult);

            properties.forEach(function(property) {
                // Ignore the summary's own values
                if (property !== "summary") {
                    // if dummy "values" property doesn't exist, create it
                    if (!statistics[property]["values"]) {
                        statistics[property]["values"] = [];
                    }
                    // If "metadata" sub-object doesn't exist, create it
                    if (!statistics[property]["metadata"]) {
                        statistics[property]["metadata"] = {};
                    }
                    // Map "count" to "overallCount" to be aligned with the string fields
                    if (field === "count") {
                        statistics[property]["metadata"]["overallCount"] = parseInt(typeResult[property]);
                    } else if (typeResult[property].indexOf(".") > -1) {
                        // Parse to float
                        statistics[property]["metadata"][field] = parseFloat(typeResult[property]);
                    } else {
                        // Parse to integer
                        statistics[property]["metadata"][field] = parseInt(typeResult[property]);
                    }
                }
            });
        });

        // Create string fields statistics
        stringFields.forEach(function(field) {
            var tempArray = [],
                other = { value: "Other", count: 0 },
                counter = 0,
                sum = 0,
                results = dataframe.groupBy(field).count().sort(self.sqlFunctions.desc("count")).toJSON().collect();

            // Parse and reformat JSON object from Spark
            results.forEach(function(result) {
                var temp = JSON.parse(result);
                // Check if should ignore the cutOffThreshold
                if (respectThreshold) {
                    // Cut off results according to given threshold
                    if (counter < cutOffThreshold) {
                        tempArray.push({ value: temp[field], count: temp.count });
                    } else {
                        other.count += temp.count;
                    }
                } else {
                    tempArray.push({ value: temp[field], count: temp.count });
                }
                sum += temp.count;
                counter++;
            });

            // If there are "Other" values, add to to array
            if (other.count > 0) {
                tempArray.push(other);
            }

            // Fill final results
            statistics[field].values = tempArray;
            statistics[field].metadata = {
                valueCount: counter,
                overallCount: sum
            }
        });

        return { fields: statistics };

    }

};

// Check if the process is a worker process
if (cluster.isWorker) {

    // Store worker id
    contextCache.workerId = cluster.worker.id;

    // Handle nessages
    process.on('message', function(message) {

        if (message.type) {
            switch (message.type.toLowerCase()) {

                case "create":

                    if (message.data && message.data.config) {

                        var contextConfig = message.data.config;

                        // Create context object instance with the provided settings
                        var sparkContext = new SparkContext(config);

                        // Set logfile path
                        contextConfig.logPath = path.join(__dirname, "../logs/" + new Date().getTime() + "-worker" + contextCache.workerId + ".log");

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
                            contextCache.context['getSchema'] = dataframeFunctions.getSchema;
                            contextCache.context['getData'] = dataframeFunctions.getData.bind(contextCache.context);
                            contextCache.context['getStatistics'] = dataframeFunctions.getStatistics.bind(contextCache.context);

                            // Send message to the master process
                            process.send({ type: message.type, ok: true, data: { config: contextConfig, logPath: contextConfig.logPath } });

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