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
    assert.object(options.morayClient, 'options.morayClient');
    assert.func(callback, 'callback');

    var bucketsList = [];
    var bucketConfigName;
    var log = options.log;
    var morayClient = options.morayClient;

    for (bucketConfigName in bucketsConfig) {
        bucketsList.push(bucketsConfig[bucketConfigName]);
    }

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
        inputs: bucketsList
    }, function onAllBucketsReindexed(reindexErr) {
        callback(reindexErr);
    });
};

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