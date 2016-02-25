var fs = require('fs');
var raml2html = require('raml2html');
var configWithDefaultTemplates = raml2html.getDefaultConfig();

raml2html.render('../docs/spark-server.raml', configWithDefaultTemplates).then(function(result) {

    fs.writeFile("../docs/spark-server.html", result, function(err) {
        if(err) {
            console.log(err);
        }
        console.log("raml2html successfully generated the API docs!");
    });
}, function(err) {
    console.log(err);
});