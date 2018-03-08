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
var verror = require('verror');

var errors = require('../errors');

/*
 * Sets up VMAPI's moray buckets, including creating them if they're
 * missing, or updating them if they already exist. Calls the 'callback'
 * function when that setup completed.
 *
 * It does not perform any reindexing of rows that would need to be reindexed
 * after a bucket was updated to add one or more indexes. To reindex rows of all
 * buckets, use the "Moray.prototype.reindexBuckets" function.
 *
 * If the setup results in an error, the first argument of the 'callback'
 * function is an Error object. The
 * 'Moray.prototype.isBucketsSetupErrorNonTransient' function can be used to
 * determine whether that error is non transient, and how to act on it depending
 * on the program's expectations and behavior.
 *
 * The "Moray.prototype.setupBuckets" function can be called more than once per
 * instance of the Moray constructor, as long as each call is made after the
 * previous setup process terminated, either successfully or with an error, by
 * calling the 'callback' function passed as a parameter. Calling this method
 * while a previous call is still in flight will throw an error.
 */
function setupBuckets(options, callback) {
    assert.object(options, 'options');
    assert.object(options.bucketsConfig, 'options.bucketsConfig');
    assert.object(options.log, 'options.log');
    assert.object(options.morayClient, 'options.morayClient');

    var bucketsList = [];
    var bucketConfig;
    var bucketsConfig = options.bucketsConfig;
    var log = options.log;
    var morayClient = options.morayClient;

    log.info({bucketsConfig: bucketsConfig},
        'Setting up moray buckets...');

    for (bucketConfig in bucketsConfig) {
        bucketsList.push(bucketsConfig[bucketConfig]);
    }

    _trySetupBuckets(bucketsList, {
        log: log,
        morayClient: morayClient
    }, function (setupBucketsErr) {
        if (setupBucketsErr) {
            log.error({ error: setupBucketsErr },
                'Error when setting up moray buckets');
        } else {
            log.info('Buckets have been setup successfully');
        }

        callback(setupBucketsErr);
    });
}

/*
 * Tries to setup moray buckets as specified by the array "buckets". Calls the
 * function "cb" when done. If there was an error, the "cb" function is called
 * with an error object as its first parameter, otherwise it is called without
 * passing any parameter.
 */
function _trySetupBuckets(buckets, options, cb) {
    assert.arrayOfObject(buckets, 'buckets');
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.morayClient, 'options.morayClient');
    assert.func(cb, 'cb');

    vasync.forEachPipeline({
        func: function setupEachBucket(newBucketConfig, done) {
            var bucketName = newBucketConfig.name;
            assert.string(bucketName, 'bucketName');

            _trySetupBucket(bucketName, newBucketConfig, options, done);
        },
        inputs: buckets
    }, cb);
}

/*
 * Tries to set up bucket with name "bucketName" to have configuration
 * "bucketConfig". The setup process includes, in the following order:
 *
 * 1. creating the bucket if it does not exist.
 *
 * 2. updating the bucket's indexes to add indexes. Indexes cannot be removed
 * because it's a backward incompitble change: if a code rollback is performed,
 * older code that would rely on the deleted indexes wouldn't be able to work
 * properly, and removing indexes will generate an error.
 *
 */
function _trySetupBucket(bucketName, bucketConfig, options, cb) {
    assert.string(bucketName, 'bucketName');
    assert.object(bucketConfig, 'bucketConfig');
    assert.object(bucketConfig.schema, 'bucketConfig.schema');
    assert.optionalObject(bucketConfig.schema.options,
        'bucketConfig.schema.options');
    if (bucketConfig.schema.options) {
        assert.optionalNumber(bucketConfig.schema.options.version,
            'bucketConfig.schema.options.version');
    }

    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.morayClient, 'options.morayClient');

    assert.func(cb, 'cb');

    var log = options.log;
    var morayClient = options.morayClient;
    var newBucketSchema = bucketConfig.schema;

    vasync.waterfall([
        function loadBucket(next) {
            log.info({bucketName: bucketName}, 'Loading moray bucket...');

            morayClient.getBucket(bucketName, function (err, oldBucketSchema) {
                if (err &&
                    verror.hasCauseWithName(err, 'BucketNotFoundError')) {
                    err = null;
                }

                next(err, oldBucketSchema);
            });
        },
        function createBucket(oldBucketSchema, next) {
            if (!oldBucketSchema) {
                log.info({bucketName: bucketName},
                    'Bucket not found, creating it...');
                morayClient.createBucket(bucketName, bucketConfig.schema,
                    function createDone(createErr) {
                        if (createErr) {
                            log.error({
                                bucketName: bucketName,
                                error: createErr.toString()
                            }, 'Error when creating bucket');
                        } else {
                            log.info('Bucket ' +
                                bucketName +
                                    ' created successfully');
                        }

                        next(createErr, oldBucketSchema);
                    });
             } else {
                log.info({bucketName: bucketName},
                    'Bucket already exists, not creating it.');
                next(null, oldBucketSchema);
            }
        },
        function updateBucketSchema(oldBucketSchema, next) {
            assert.optionalObject(oldBucketSchema, 'oldBucketSchema');

            var oldVersion = 0;
            var newVersion = 0;
            var removedIndexes = [];

            if (oldBucketSchema && oldBucketSchema.options &&
                oldBucketSchema.options.version) {
                oldVersion = oldBucketSchema.options.version;
            }

            if (newBucketSchema.options && newBucketSchema.options.version) {
                newVersion = newBucketSchema.options.version;
            }

            /*
             * If the bucket's version was bumped, update the bucket, otherwise:
             *
             * 1. the version number wasn't bumped because no change was made
             * and there's nothing to do.
             *
             * 2. the version number is lower than the current version number in
             * moray. This can be the result of a code rollback. Since we make
             * only backward compatible changes for moray buckets, and
             * decrementing a bucket's version number is an error, it's ok to
             * not change the bucket.
             */
            if (oldBucketSchema && newVersion > oldVersion) {
                removedIndexes = indexesRemovedBySchemaChange(oldBucketSchema,
                    newBucketSchema);
                if (removedIndexes.length > 0) {
                    /*
                     * Removing indexes is considered to be a backward
                     * incompatible change. We don't allow them so that after
                     * rolling back to a previous version of the code, the code
                     * can still use any index that it relies on.
                     */
                    next(new errors.InvalidIndexesRemovalError(removedIndexes));
                    return;
                }

                log.info('Updating bucket ' + bucketName + ' from ' +
                    'version ' + oldVersion + ' to version ' + newVersion +
                    '...');

                morayClient.updateBucket(bucketName, newBucketSchema,
                    function updateDone(updateErr) {
                        if (updateErr) {
                            log.error({error: updateErr},
                                'Error when updating bucket ' +
                                    bucketName);
                        } else {
                            log.info('Bucket ' + bucketName +
                                ' updated successfully');
                        }

                        next(updateErr);
                    });
            } else {
                log.info('Bucket ' + bucketName + ' already at version ' +
                    '>= ' + newVersion + ', no need to update it');
                next(null);
            }
        }
    ], cb);
}

/*
 * Returns true if the updating a moray bucket from the bucket schema
 * "oldBucketSchema" to "newBucketSchema" would imply removing at least one
 * index. Returns false otherwise.
 */
function indexesRemovedBySchemaChange(oldBucketSchema, newBucketSchema) {
    assert.object(oldBucketSchema, 'oldBucketSchema');
    assert.object(newBucketSchema, 'newBucketSchema');

    var oldBucketIndexNames = [];
    var newBucketIndexNames = [];

    if (oldBucketSchema.index) {
        oldBucketIndexNames = Object.keys(oldBucketSchema.index);
    }

    if (newBucketSchema.index) {
        newBucketIndexNames = Object.keys(newBucketSchema.index);
    }

    var indexesRemoved =
        oldBucketIndexNames.filter(function indexMissingInNewSchema(indexName) {
            return newBucketIndexNames.indexOf(indexName) === -1;
        });

    return indexesRemoved;
}

function isBucketsSetupErrorTransient(err) {
    assert.object(err, 'err');
    assert.string(err.name, 'err.name');

    var nonTransientErrName;
    var NON_TRANSIENT_ERROR_NAMES = [
        /* Errors sent by the moray server */
        'InvalidBucketConfigError',
        'InvalidBucketNameError',
        'InvalidIndexDefinitionError',
        'NotFunctionError',
        'BucketVersionError',
        /* Custom errors generated by this module */
        'InvalidIndexesRemovalError'
    ];

    for (var idx in NON_TRANSIENT_ERROR_NAMES) {
        nonTransientErrName = NON_TRANSIENT_ERROR_NAMES[idx];
        if (verror.hasCauseWithName(err, nonTransientErrName)) {
            return false;
        }
    }

    return true;
}

module.exports = {
    setupBuckets: setupBuckets,
    isBucketsSetupErrorTransient: isBucketsSetupErrorTransient
};
