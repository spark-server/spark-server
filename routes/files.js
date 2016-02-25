var express = require('express');
var fs2obj = require('fs2obj');
var multer  = require('multer');
var router = express.Router();
var commonFunctions = require('../lib/commonFunctions');

module.exports = function(config, logger) {

    var storage = multer.diskStorage({
        destination: function (req, file, cb) {
            cb(null, config.fileCache);
        },
        filename: function (req, file, cb) {
            console.log(file.originalname);
            cb(null, commonFunctions.uniqueId(20));
        }
    });

    var upload = multer({ storage: storage, limits: { fieldSize: 50 } });

    // List all files
    router.get('/', function (req, res) {

        var fileArray = [],
            files = fs2obj(config.fileCache);

        files.items.forEach(function(file) {
            fileArray.push({ fileId: file.name, filePath: config.fileCache+"/"+file.name, fileSize: file.size });
        });

        res.send({ files: fileArray, count: fileArray.length });

    });

    // Return file as binary
    router.get('/:fileName', function (req, res) {

        var fileName = req.params.fileName,
            rootPath = config.fileCache+"/";

        // Workaround for using folders below the /files path
        // Separator is -----
        if (fileName.indexOf("-----") > -1) {
            var temp = fileName.split("-----");
            fileName = temp.pop();
            rootPath = rootPath + temp.join("/") + "/";
        }

        if (fileName.indexOf("../") > -1 || fileName.indexOf("/") > -1) {
            res.status(500).json({ message: "Invalid file name!" })
        } else {

            var options = {
                root: rootPath,
                dotfiles: 'deny',
                headers: {
                    'x-timestamp': Date.now(),
                    'x-sent': true
                }
            };

            res.sendFile(fileName, options, function (err) {
                if (err) {
                    console.log(err);
                    res.status(err.status).end();
                }
            });

        }

    });

    // Create application
    router.post('/', upload.array('file', 5), function (req, res) {

        var response = {};
        response.files = [];

        req.files.forEach(function(file) {
            response.files.push({fileId: file.filename, filePath: file.path, fileSize: file.size, originalFileName: file.originalname});
        });

        res.status(201).json(response);

    });

    // Delete an application
    router.delete('/:fileName', function (req, res) {

        var fileName = req.params.fileName;

        if (fileName.indexOf("../") > -1 || fileName.indexOf("/") > -1) {
            res.status(500).json({ message: "Invalid file name!" })
        } else {
            var filePath = config.fileCache+"/"+fileName;
            if (commonFunctions.fileExists(filePath)) {
                commonFunctions.deleteFile(filePath, function(err) {
                    if (err) {
                        res.status(500).json({ ok: false, message: err });
                    } else {
                        res.json({ ok: true });
                    }
                });
            } else {
                res.status(404).json({ ok: false, message: "File " + fileName + " was not found!" });
            }
        }

    });

    return router;

};