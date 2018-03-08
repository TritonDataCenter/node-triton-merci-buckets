/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var assert = require('assert-plus');
var jsprim = require('jsprim');
var moray = require('moray');
var uuid = require('uuid');
var vasync = require('vasync');

var DEFAULT_MORAY_IP = '10.99.99.17';

function creatTestMorayClient(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

    var morayConfig = {
        connectTimeout: 200,
        host: DEFAULT_MORAY_IP,
        log: options.log,
        port: 2020,
        retry: {
            retries: 2,
            minTimeout: 500
        }
    };

    if (process.env.MORAY_IP !== undefined) {
        morayConfig.host = process.env.MORAY_IP;
    }

    return moray.createClient(morayConfig);
}

function writeObjects(morayClient, bucketName, valueTemplate, nbObjects,
    callback) {
    assert.object(morayClient, 'morayClient');
    assert.string(bucketName, 'bucketName');
    assert.object(valueTemplate, 'valueTemplate');
    assert.number(nbObjects, 'nbObjects');
    assert.func(callback, 'callback');

    var i;

    var objectKeys = [];
    for (i = 0; i < nbObjects; ++i) {
        objectKeys.push(uuid.v4());
    }

    vasync.forEachParallel({
        func: function writeObject(objectUuid, done) {
            var newObjectValue = jsprim.deepCopy(valueTemplate);
            newObjectValue.uuid = objectUuid;
            morayClient.putObject(bucketName, objectUuid, newObjectValue, {
                /*
                 * noBucketCache: true is needed so that when putting objects in
                 * moray after a bucket has been deleted and recreated, it
                 * doesn't use an old bucket schema and determine that it needs
                 * to update an _rver column that doesn't exist anymore.
                 */
                noBucketCache: true
            }, done);
        },
        inputs: objectKeys
    }, callback);
}

module.exports = {
    creatTestMorayClient: creatTestMorayClient,
    writeObjects: writeObjects
};
