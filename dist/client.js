'use strict';

let url = require('url');
let http = require('http');
let https = require('https');

var httpAgent = new http.Agent({
    keepAlive: true,
    maxSockets: 128,
    keepAliveMsecs: 8000
});
var httpsAgent = new https.Agent({
    keepAlive: true,
    maxSockets: 128,
    keepAliveMsecs: 8000
});

function get(server, user, password) {
    return function (method, _url) {
        let postObj = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : null;

        var body = postObj ? JSON.stringify(postObj) : null;

        console.log('' + server + _url);

        let aurl = url.parse('' + server + _url);
        let isHttp = aurl.protocol === 'http:';
        let options = {
            agent: isHttp ? httpAgent : httpsAgent,
            hostname: aurl.hostname,
            port: aurl.port,
            path: aurl.path,
            method: method,
            headers: {
                "Authorization": 'Basic ' + new Buffer(user + ":" + password).toString("base64")
            }
        };
        if (body) {
            options.headers["Content-Type"] = 'application/json';
            options.headers["Content-Length"] = Buffer.byteLength(body);
        }

        return cb => {
            var req = (isHttp ? http : https).request(options, function (res) {
                let parts = [];

                res.setEncoding('utf8');
                res.on('data', function (data) {
                    parts.push(data);
                });
                res.on('error', console.error);
                res.on('end', function () {
                    try {
                        if (res.statusCode === 409) {
                            // Ignore 409 conflict
                        } else if (res.statusCode === 200) {
                            // Ignore 200 success
                        } else if (res.statusCode === 201) {
                            // Ignore 201 created
                        } else if (res.statusCode === 301) {
                            // Ignore 301 moved
                        } else {
                            console.log(parts);
                            throw new Error('HTTP status ' + res.statusCode);
                        }
                        if (parts.length > 0) {
                            let buffer = parts.join('');
                            let json = JSON.parse(buffer);
                            cb(null, {
                                status: res.statusCode,
                                headers: res.headers,
                                data: json
                            });
                        } else {
                            cb(null, {
                                status: res.statusCode,
                                headers: res.headers
                            });
                        }
                    } catch (ex) {
                        cb(ex);
                    }
                });
            });
            req.on('error', function (e) {
                cb(e);
            });
            req.end(body);
        };
    };
}

module.exports = function client(server, user, password, db) {
    let api = get(server, user, password);
    return {
        keys: function keys() {
            let startKey = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : null;
            let batch = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : 5000;

            let allDocs = api('GET', '/' + db + '/_all_docs?limit=' + (startKey ? batch + 1 : batch) + '&start_key=' + JSON.stringify(startKey));
            return cb => allDocs((err, res) => {
                if (err) return cb(err);
                cb(null, res.data.rows.map(x => x.key));
            });
        },
        headDoc: function headDoc(key) {
            let head = api('HEAD', '/' + db + '/' + key);
            return cb => head((err, res) => {
                if (err) return cb(err);
                cb(null, res.headers);
            });
        },
        headDocRange: function headDocRange(startKey, endKey) {
            let head = api('HEAD', '/' + db + '/_all_docs?include_docs=true&start_key=' + JSON.stringify(startKey) + '&end_key=' + JSON.stringify(endKey));
            return cb => head((err, res) => {
                if (err) return cb(err);
                cb(null, res.headers);
            });
        },
        docRange: function docRange(startKey, endKey) {
            let range = api('GET', '/' + db + '/_all_docs?include_docs=true&start_key=' + JSON.stringify(startKey) + '&end_key=' + JSON.stringify(endKey));
            return cb => range((err, res) => {
                if (err) return cb(err);
                cb(null, res.data);
            });
        },
        testDocRange: function testDocRange(startKey, endKey) {
            let range = api('GET', '/' + db + '/_all_docs?include_docs=true&start_key=' + JSON.stringify(startKey) + '&end_key=' + JSON.stringify(endKey), false);
            return cb => range((err, res) => {
                if (err) return cb(err);
                cb(null, res.data);
            });
        },
        doc: function doc(key) {
            let head = api('GET', '/' + db + '/' + encodeURIComponent(key));
            return cb => head((err, res) => {
                if (err) return cb(err);
                cb(null, res.data);
            });
        },
        saveBulk: function saveBulk(docs) {
            let head = api('POST', '/' + db + '/_bulk_docs', {
                docs: docs
            });
            return cb => head((err, res) => {
                if (err) return cb(err);
                cb(null, res.data);
            });
        },
        save: function save(doc) {
            let head = api('PUT', '/' + db + '/' + encodeURIComponent(doc._id), doc);
            return cb => head((err, res) => {
                if (err) return cb(err);
                cb(null, res.data);
            });
        },
        testDoc: function testDoc(key) {
            let head = api('GET', '/' + db + '/' + encodeURIComponent(key), false);
            return cb => head((err, res) => {
                if (err) return cb(err);
                cb(null, res.data);
            });
        },
        count: function count() {
            let startKey = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : null;

            let head = api('GET', '/' + db);
            return cb => head((err, res) => {
                if (err) return cb(err);
                cb(null, res.data.doc_count);
            });
        }
    };
};