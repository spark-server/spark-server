var express = require('express');
var mime = require('mime');
var fs = require('fs');
var router = express.Router();

module.exports = function(config, logger) {

    if (config.sparkDistribution.enabled) {

        var pathArray = config.sparkDistribution.path.split("/");
        var fileName = pathArray[pathArray.length-1];

        // List all files
        router.get('/'+fileName, function (req, res) {

            var mimetype = mime.lookup(fileName);

            res.setHeader('Content-Disposition', 'attachment; filename=' + fileName);
            res.setHeader('Content-Type', mimetype);

            var filestream = fs.createReadStream(config.sparkDistribution.path);
            filestream.pipe(res);

        });

    }

    return router;

};