var path = require('path');

module.exports = {
    assemblyJar: process.env.ASSEMBLY_JAR,
    fileCache: path.normalize(__dirname+'/../fileCache'),
    tmpCache: path.normalize(__dirname+'/../tmpCache'),
    sparkDistribution: {
        enabled: (process.env.SPARK_PACKAGE_PATH ? true : false),
        path: process.env.SPARK_PACKAGE_PATH,
        uri: "/spark"
    },
    logLevel: "INFO",
    spark: {
        portRange: {
            start: (process.env.POST_RANGE_START ? parseInt(process.env.POST_RANGE_START) : 10000),
            end: (process.env.POST_RANGE_END ? parseInt(process.env.POST_RANGE_END) : 10100)
        },
        createContextTimeout: 30000,
        standardContextConfig: {
            createSQLContext: true,
            properties: {
                "spark.driver.cores": "1",
                "spark.driver.memory": "1g",
                "spark.executor.memory": "2g",
                "spark.shuffle.sort.bypassMergeThreshold": "8"
            }
        },
        dataframeOperations: {
            stats: "getStatistics",
            data: "getData",
            schema: "getSchema"
        },
        statistics: {
            showAllThreshold: 25,
            cutOffThreshold: 25
        },
        bindInterface: (process.env.BIND_INTERFACE ? process.env.BIND_INTERFACE : "en0")
    },
    vmContextConfiguration: {
        filename: "sparkContextWorker.js",
        displayErrors: true,
        timeout: 30000
    },
    api: {
        port: (process.env.API_PORT ? parseInt(process.env.API_PORT) : 3000),
        version: 1
    }
};