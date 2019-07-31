'use strict';

let commandLineArgs = require('command-line-args');

// print process.argv
process.argv.forEach(function (val, index, array) {
    console.log(index + ': ' + val);
  });

let optionDefinitions = [{
    name: 'verbose',
    alias: 'v',
    type: Boolean
}, {
    name: 'server',
    alias: 's',
    type: String
}, {
    name: 'batch',
    alias: 'b',
    type: Number
}, {
    name: 'user',
    alias: 'u',
    type: String
}, {
    name: 'password',
    alias: 'p',
    type: String
}, {
    name: 'srcdb',
    type: String
}, {
    name: 'dstdb',
    type: String
}, {
    name: 'startKey',
    type: String
}];
let options = commandLineArgs(optionDefinitions);

let detect = require('./detect');
detect(options.server, options.user, options.password, options.srcdb, options.dstdb, {
    batch: options.batch || 1000,
    startKey: options.startKey || null
});