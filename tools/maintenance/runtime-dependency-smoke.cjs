'use strict';
// Run inside each final image, after npm installation. No app/database/Redis/Cron initialization.
const assert = require('assert').strict;
const colors = require('irc-colors');
const irc = require('matrix-org-irc');
const manifest = require('../../package.json');
const installed = require('irc-colors/package.json');
assert.equal(manifest.dependencies['irc-colors'], '1.5.0');
assert.equal(installed.version, '1.5.0');
assert.equal(typeof irc.Client, 'function');
assert.equal(colors.stripColors('\x0304red'), 'red');
// Match IRC's real third-party startup import; do not load its configured logger or instantiate a connection.
console.log(JSON.stringify({ node: process.version, ircColors: installed.version, startupDependencyLoad: 'passed' }));
