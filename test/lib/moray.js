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

/*
 * This value is the result of running "vmadm get $(vmadm lookup -1
 * alias=binder0) | json nics.0.ip" in my COAL's GZ. It can be overriden by
 * setting the BINDER_IP environment variable.
 */
var DEFAULT_BINDER_IP = '10.99.99.11';

function creatTestMorayClient(options) {
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

    /*
     * We want to use a configuration that makes the Moray client use SRV
     * records so that cueball can perform te load balancing of requests itself
     * instead of relying on HAProxy to do that. Relying on HAProxy means that
     * it is very likely that most requests to Moray will hit the same Moray
     * instance, and that we won't exercise behaviors that are only triggered
     * when hitting all Moray instances evenly (e.g. stale buckets cache).
     */
    var morayConfig = {
        cueballOptions: {
            resolvers: [DEFAULT_BINDER_IP]
        },
        log: options.log,
        srvDomain: 'moray.coal.joyent.us'
    };

    if (process.env.BINDER_IP !== undefined) {
        morayConfig.cueballOptions = {
            resolvers: [process.env.BINDER_IP]
        };
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
