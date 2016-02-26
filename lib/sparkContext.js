var JavaInstance = require('./javaInstance');
var fs = require('fs');
var path = require('path');
var config = require('./../config/config');

// Create Java Instance
var javaInstance = new JavaInstance();

SparkContext = function SparkContext(globalConfiguration){

    var self = this;

    // Global configuration
    self.globalConfiguration = globalConfiguration;

    // Java instance
    self.java = javaInstance.get();

    // Logger
    self.logger = null;

    // Spark classes object
    self.sparkClasses = {};

    // Instantiate NodeSparkSubmit
    self.sparkSubmit = null;

    // Instantiate sqlFunctions
    self.sqlFunctions = null;

    // SparkContext placeholder (singleton pattern / per SparkContext)
    self.sparkContext = null;

    self.javaSparkContext = null;

    // SparkContext placeholder (singleton pattern / per SparkContext)
    self.sqlContext = null;

    var privateMethods = {
        setup: function() {

            if (typeof self.globalConfiguration.assemblyJar != 'string' || !fs.statSync(path.join(self.globalConfiguration.assemblyJar)).isFile()) {
                console.error("Error: ASSEMBLY_JAR environment variable does not contain valid path");
            } else {

                // Put assembly jar in the CLASSPATH
                self.java.classpath.push(self.globalConfiguration.assemblyJar);

                // Instantiate SparkContext
                self.sparkClasses.SparkContext = self.java.import("org.apache.spark.SparkContext");

                // Instantiate JavaSparkContext
                self.sparkClasses.JavaSparkContext = self.java.import("org.apache.spark.api.java.JavaSparkContext");

                // Instantiate SQLContext
                self.sparkClasses.SQLContext = self.java.import("org.apache.spark.sql.SQLContext");

                // Instantiate SparkConf
                self.sparkClasses.SparkConf = self.java.import("org.apache.spark.SparkConf");

                // Instantiate SQLFunctions
                self.sparkClasses.sqlFunctions = self.java.import('org.apache.spark.sql.functions');

                // Configure logging
                var level = self.java.import('org.apache.log4j.Level');
                self.logger = self.java.import('org.apache.log4j.Logger');
                self.logger.getRootLogger().setLevel(level.WARN);

            }

        },
        createSQLContext: function() {

            // If not already created, create sparkSQLContext instance
            if (!self.sqlContext) {
                // Check if sparkContext is already created
                if (self.sparkContext) {
                    // Create SparkSQLContext
                    self.sqlContext = new self.sparkClasses.SQLContext(self.sparkContext);
                    self.sqlFunctions = self.sparkClasses.sqlFunctions;
                } else {
                    self.logger.warn("sqlContext cannot be created due to missing sparkContext");
                }
            }

        },
        buildConfiguration: function(appConfiguration) {

            var commandLineArguments = [];

            var configurationProperties = {
                "spark.rootLogger.level": config.logLevel,
                "spark.scheduler.mode": "FAIR",
                "spark.scheduler.allocation.file": path.normalize(__dirname+'/../config/spark_pools.xml'),
                "spark.scheduler.pool": "default"
            };

            // Fill packages if configured
            if (appConfiguration.packages && appConfiguration.packages.length > 0) {
                var packages = [];
                appConfiguration.packages.forEach(function(pkg){
                    // Check if it's a string, and contains two colons
                    if (typeof pkg === "string" && (pkg.match(/:/g) || []).length === 2) {
                        packages.push(pkg)
                    }
                });
                commandLineArguments = commandLineArguments.concat(['--packages', packages.join(",")]);
            }

            // Check if local repository is configured, if so -> use it
            if (appConfiguration.localRepository) {
                commandLineArguments = commandLineArguments.concat(['--repositories', appConfiguration.localRepository]);
            }

            // Check for application name
            if (appConfiguration.applicationName) {
                commandLineArguments = commandLineArguments.concat(['--name', appConfiguration.applicationName]);
            }

            // Check for Master URL
            if (appConfiguration.masterUrl) {
                commandLineArguments = commandLineArguments.concat(['--master', appConfiguration.masterUrl]);
            }

            // Check for configuration properties
            if (appConfiguration.properties && Object.keys(appConfiguration.properties).length > 0) {
                // Iterate
                Object.keys(appConfiguration.properties).forEach(function(property) {
                    // Check if it's a valid Spark configuration property
                    if (property.indexOf("spark.") > -1) {
                        // Add to configuration, overwrite defaults if necessary
                        configurationProperties[property] = appConfiguration.properties[property];
                    }
                });
            }

            // Add each configured property to the startup parameters
            Object.keys(configurationProperties).forEach(function(property) {
                commandLineArguments = commandLineArguments.concat(['--conf' , property + '=' + configurationProperties[property]]);
            });

            return commandLineArguments;

        }

    };

    // Trigger setup
    privateMethods.setup();

    return {
        create : function(appConfiguration, callback) {

            // If not already created, return sparkContext instance
            if (!self.sparkContext) {

                // Import System class
                var System = self.java.import('java.lang.System');

                // Prepare redirection of Spark's JVM stderr output to a predefined logfile
                var FileOutputStream = self.java.newInstanceSync('java.io.FileOutputStream', appConfiguration.logPath);
                var PrintStream = self.java.newInstanceSync('java.io.PrintStream', FileOutputStream);

                // Redirect stderr output to logfile
                System.setErr(PrintStream);

                // Apply Spark arguments
                var args =
                    // fake presence of a main class so that SparkSubmitArguments doesn't bail. (It won't be run)
                    ['--class', 'org.apache.spark.repl.Main'];

                args = args.concat(privateMethods.buildConfiguration(appConfiguration));

                // Add SparkShell class
                args = args.concat(['spark-shell']);

                // Instantiate NodeSparkSubmit
                self.sparkSubmit = self.java.import("org.apache.spark.deploy.NodeSparkSubmit");

                // Start application / context
                self.sparkSubmit.apply(self.java.newArray("java.lang.String", args));

                // Instantiate SparkConf
                var conf = new self.sparkClasses.SparkConf();

                // Create SparkContext
                self.sparkContext = new self.sparkClasses.SparkContext(conf);

                // Create JavaSparkContext
                self.javaSparkContext = self.sparkClasses.JavaSparkContext.fromSparkContext(self.sparkContext);

                if (appConfiguration.createSQLContext || self.globalConfiguration.createSQLContext) {

                    // Create SQLContext from SparkContext
                    privateMethods.createSQLContext();

                }

                self.sparkContext.setLogLevel(globalConfiguration.logLevel);

                callback(null, self);

            } else {

                callback({ message: "SparkContext already exists!" }, null);

            }

        }

    }

};

SparkContext.prototype.destroy = function(callback) {

    var self = this;

    // Stop SparkContext
    self.sparkContext.stop();

    // Remove references
    delete self.java;
    delete self.logger;
    delete self.sparkClasses;
    delete self.sparkContext;
    delete self.sqlContext;
    delete self.sqlFunctions;
    delete self.sparkSubmit;

    callback(null, { ok: true });
}

module.exports = SparkContext;