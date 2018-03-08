/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var assert = require('assert-plus');
var vasync = require('vasync');

var performBackedOffProcess =
    require('./backedoff-process').performBackedOffProcess;
/*
 * Reindexes all buckets and calls "callback" when it's done.
 *
 * @param {Function} callback - a function called when either the reindexing
 *   process is complete for all buckets, or when an error occurs. It is called
 *   as "callback(null)" if the reindexing process completed with no error, or
 *   "callback(err)"" if the error "err" occurred.
 */
function reindexBuckets(bucketsConfig, options, callback) {
    assert.object(bucketsConfig, 'bucketsConfig');
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.optionalNumber(options.maxAttempts, 'options.maxAttempts');
    assert.object(options.morayClient, 'options.morayClient');
    assert.func(callback, 'callback');

    var bucketConfigName;
    var bucketsList = [];
    var bucketsReindexStatus = {state: 'STARTED'};
    var log = options.log;
    var maxAttempts = options.maxAttempts;
    var morayClient = options.morayClient;

    for (bucketConfigName in bucketsConfig) {
        bucketsList.push(bucketsConfig[bucketConfigName]);
    }

    performBackedOffProcess('buckets reindex',
        _tryReindexBuckets.bind(null, bucketsList, bucketsReindexStatus, {
            log: log,
            morayClient: morayClient
        }), {
            isErrTransientFun: function isReindexErrorTransient(/* err */) {
                /*
                 * Reindexing errors are always transient.
                 */
                return true;
            },
            log: log,
            maxAttempts: maxAttempts
        }, callback);

    return bucketsReindexStatus;
}

function _tryReindexBuckets(buckets, status, options, cb) {
    assert.arrayOfObject(buckets, 'buckets');
    assert.object(status, 'status');
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.morayClient, 'options.morayClient');
    assert.func(cb, 'cb');

    var log = options.log;
    var morayClient = options.morayClient;

    vasync.forEachPipeline({
        func: function reindex(bucketConfig, done) {
            assert.object(bucketConfig, 'bucketConfig');
            assert.string(bucketConfig.name, 'bucketConfig.name');

            var bucketName = bucketConfig.name;

            log.info('Reindexing bucket ' + bucketName + '...');

            reindexBucket(bucketName, morayClient,
                function reindexDone(reindexErr) {
                    if (reindexErr) {
                        log.error({err: reindexErr},
                            'Error when reindexing bucket ' + bucketName);
                    } else {
                        log.info('Bucket ' + bucketName +
                            ' reindexed successfully');
                    }

                    done(reindexErr);
                });
        },
        inputs: buckets
    }, function onAllBucketsReindexed(bucketsReindexErr) {
        if (bucketsReindexErr) {
            status.latestError = bucketsReindexErr;
            status.state = 'ERROR';
        } else {
            delete status.latestError;
            status.state = 'DONE';
        }

        cb(bucketsReindexErr);
    });
}

/*
 * Reindexes all objects in the bucket with name "bucketName" and calls the
 * function "callback" when it's done.
 *
 * @param bucketName {String} Name of the bucket to reindex
 * @param morayClient {MorayClient}
 * @param callback {Function} `function (err)`
 */
function reindexBucket(bucketName, morayClient, callback) {
    assert.string(bucketName, 'bucketName');
    assert.object(morayClient, 'morayClient');
    assert.func(callback, 'callback');

    morayClient.reindexObjects(bucketName, 100,
        function onReindexBucketDone(reindexErr, res) {
            if (reindexErr || res.processed < 1) {
                callback(reindexErr);
                return;
            }

            reindexBucket(bucketName, morayClient, callback);
        });
}

module.exports = {
    reindexBuckets: reindexBuckets
};
