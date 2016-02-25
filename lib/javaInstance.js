var java = require('java');
var path = require('path');

function JavaInstance() {
    java.asyncOptions = {
        syncSuffix: '',           // Synchronous methods the base name
        asyncSuffix: undefined,   // don't generate async (callback-style) wrappers
        promiseSuffix: "Promised",  // generate promise wrappers
        promisify: require("bluebird").promisify
    };

    // Add NodeSparkSubmit to classpath
    java.classpath.push(path.normalize(__dirname+'/../build'));

    return {
        get: function() {
            return java;
        }
    }
}

module.exports = JavaInstance;
