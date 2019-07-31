'use strict';

let async = require('async');
let chalk = require('chalk');
let client = require('./client');
let humDur = require('humanize-duration');

function eta(total) {
    let start = Date.now();
    var prevTime = Date.now();
    var prevCount = 0;

    return current => {
        let time = Date.now();
        let left = total - current;
        let dur = time - start;
        let ipms = dur / current;

        let diffTime = time - prevTime;
        let diffCount = current - prevCount;
        let cps = diffCount / diffTime;
        prevTime = time;
        prevCount = current;
        return humDur(left * ipms, { largest: 2, round: true }) + ' remaining (' + cps.toFixed(3) + 'k/s)';
    };
}

function logError(fn, desc) {
    return cb => fn((err, res) => {
        if (err) {
            if (err.code === 'ETIMEDOUT') console.error(chalk.red(desc), err.code);
            if (/HTTP status \d+/.test(err.message)) console.error(chalk.red(desc), err.message);else console.error(chalk.red(desc), err);
        }
        cb(err, res);
    });
}

function retry(fn) {
    return cb => async.retry({ times: 10, interval: 5000 }, fn, cb);
}
function retryHttp(fn) {
    return cb => async.retry({ times: 10, interval: 5000, errorFilter: message => /ETIMEDOUT/.test(message) }, fn, cb);
}

module.exports = function detect(server, user, password, srcdb, dstdb, options) {
    let src = client(server, user, password, srcdb);
    let dst = client(server, user, password, dstdb);
    var total = 0;
    console.log('Here');
    src.count()((err, docCount) => {
        console.log('And Here');
        let e = eta(docCount);
        console.log(options.startKey);
        forAllKeys(src, options.startKey, options.batch)((keys, nextKeys) => {
            // Part.
            console.error(chalk.cyan('Processing ' + keys.length.toLocaleString() + ' of ' + docCount.toLocaleString() + ' keys...'));

            async.mapLimit(keys, 32, (key, next) => {
                // Retrieve document.
                let retryDoc = retry(logError(src.doc(key), 'Error occurred at key ' + key + ' ' + encodeURIComponent(key)));
                retryDoc((err, doc) => {
                    // Got a result.
                    let d = Object.assign({}, doc);
                    delete d['_rev'];
                    next(err, d);
                });
            }, (err, docs) => {
                // Part processed.
                if (err) throw err;
                let retrySave = retry(logError(dst.saveBulk(docs), 'Error occurred during post start key ' + keys[0] + ' ' + encodeURIComponent(keys[0])));
                retrySave(err => {
                    if (err) throw err;
                    total += keys.length;
                    console.error(chalk.white((total / docCount * 100).toFixed() + '% ' + chalk.grey('' + keys[keys.length - 1])));
                    console.error(chalk.white('Processed ' + total.toLocaleString() + ' keys'));
                    console.error(chalk.white('' + e(total)));
                    console.error();
                    nextKeys();
                });
            });
            // async.eachOfLimit(keys, 32, (key, _, next) => {
            //     // Retrieve document.
            //     const retryDoc = retry(logError(src.doc(key), `Error occurred at key ${key} ${encodeURIComponent(key)}`));
            //     retryDoc((err, doc) => {
            //       // Got a result.
            //       const rest = Object.assign({}, doc);
            //       const retrySave = retry(logError(dst.save(rest), `Error occurred during post key ${key} ${encodeURIComponent(key)}`));
            //       retrySave((err) => {
            //         next(err);
            //       });
            //     });
            // }, () => {
            //     // Part processed.
            //     total += keys.length;
            //     console.error(chalk.white(`${((total/docCount)*100).toFixed()}% ${chalk.grey(`${keys[keys.length-1]}`)}`));
            //     console.error(chalk.white(`Processed ${total.toLocaleString()} keys`));
            //     console.error(chalk.white(`${e(total)}`));
            //     console.error();
            //     nextKeys();
            // });
        }, err => {
            // Completed.
            if (err) return console.error(err);
        });
    });
};

function forAllKeys(c) {
    let startKey = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : null;
    let batch = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : 1000;

    var retry = 0;
    function iter(startKey, cbIter, cbErr) {
        c.keys(startKey, batch)((err, keys) => {
            if (err) {
                retry++; 
                console.log(err);

                if(retry <= 2) {
                    iter(startKey, cbIter);
                }
                else 
                    return;
            }

            retry = 0;
            if (startKey) {
                // skip previous end key
                keys = keys.slice(1);
            }
            cbIter(keys, () => {
                if (keys.length) {
                    iter(keys[keys.length - 1], cbIter);
                }
            });
        });
    }
    return (cbIter, cbErr) => iter(startKey, cbIter, cbErr);
}