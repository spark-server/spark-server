var crypto = require('crypto');
var fs = require('fs');
var os = require('os');
var babel = require("babel-core");

module.exports = {
    uniqueId: function (idLength) {
        return crypto.randomBytes(idLength).toString('hex');
    },
    shallowCopy: function(origin) {
        return Object.keys(origin).reduce(function(c,k){c[k]=origin[k];return c;},{});
    },
    fileExists: function (filePath){
        try{
            fs.statSync(filePath);
        }catch(err){
            if(err.code == 'ENOENT') return false;
        }
        return true;
    },
    deleteFile: function (filePath, callback){
        return fs.unlink(filePath, callback)
    },
    getIPAddress: function (bindInterface) {

        var ifaces = os.networkInterfaces(),
            addresses = {};

        Object.keys(ifaces).forEach(function (ifname) {
            ifaces[ifname].forEach(function (iface) {
                if ('IPv4' !== iface.family || iface.internal !== false) {
                    // skip over internal (i.e. 127.0.0.1) and non-ipv4 addresses
                    return;
                }
                addresses[ifname] = iface.address
            });
        });

        return (addresses[bindInterface] ? addresses[bindInterface] : "127.0.0.1");

    },
    wrapAndTranspile: function(code) {

        var runFunctionId = "run_"+this.uniqueId(20);

        // Wrap the code
        code = "'use strict'; if (this['" + runFunctionId + "']) { delete this['" + runFunctionId + "']; }; if (this['_asyncToGenerator']) { delete this['_asyncToGenerator']; }; async function " + runFunctionId + "() { try { " + code.replace(/var /g, "this.") + "; this.callback(null); } catch(error) { this.callback(error); } }; " + runFunctionId + ".apply(this)";

        // Transpile code ES7 -> ES5 and return
        return babel.transform(code, { "ast": false, "presets": ["stage-0"] }).code;

    }
};