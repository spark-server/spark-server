module.exports = function request(opts) {
    return new Promise(function(resolve, reject) {
        require('request')(opts, function(err, resp, body) {
            if(err) {
                reject(err);
            } else {
                resp.body = body;
                resolve(resp);
            }
        });
    });
}