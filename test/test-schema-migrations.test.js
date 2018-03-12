/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var assert = require('assert-plus');
var bunyan = require('bunyan');
var test = require('tape');
var vasync = require('vasync');
var verror = require('verror');

var MorayBucketsInitializer = require('../index').MorayBucketsInitializer;
var testMoray = require('./lib/moray.js');

/*
 * The number of test objects is chosen so that it's larger than the default
 * page for Moray requests (which is currently 1000). 2001 objects means that at
 * least 3 Moray requests are necessary to reindex all records from the test
 * moray buckets.
 */
var NB_TEST_OBJECTS = 2001;

var TEST_LOGGER = bunyan.createLogger({
    name: 'test-schema-migrations'
});

function getAllObjectsRowVer(morayClient, bucketName, callback) {
    assert.object(morayClient, 'morayClient');
    assert.string(bucketName, 'bucketName');
    assert.func(callback, 'callback');

    var allRecords = [];

    var findAllObjectsReq = morayClient.sql('select _rver from ' + bucketName);

    findAllObjectsReq.once('error', function onSqlError(sqlErr) {
        cleanup();
        callback(sqlErr);
    });

    findAllObjectsReq.on('record', function onRecord(record) {
        allRecords.push(record);
    });

    findAllObjectsReq.once('end', function onGotAllRecords() {
        cleanup();
        callback(null, allRecords);
    });

    function cleanup() {
        findAllObjectsReq.removeAllListeners('error');
        findAllObjectsReq.removeAllListeners('record');
        findAllObjectsReq.removeAllListeners('end');
    }
}

function testMigrationToBucketsConfig(bucketsConfig, options, t, callback) {
    assert.object(bucketsConfig, 'bucketsConfig');
    assert.object(options, 'options');
    assert.arrayOfObject(options.expectedResults, 'options.expectedResults');
    assert.object(t, 't');
    assert.func(callback, 'callback');

    var morayBucketsInitializer;
    var morayClient;

    vasync.pipeline({funcs: [
        function connectToMoray(_, next) {
            morayClient = testMoray.creatTestMorayClient({
                log: TEST_LOGGER
            });

            morayClient.once('connect', next);
            morayClient.once('error', next);
        },
        function setupMorayBuckets(_, next) {
            morayBucketsInitializer = new MorayBucketsInitializer({
                bucketsConfig: bucketsConfig,
                log: TEST_LOGGER,
                morayClient: morayClient
            });

            morayBucketsInitializer.start();

            morayBucketsInitializer.on('done',
                function onMorayBucketsInit() {
                    t.ok(true,
                        'moray initialization should be successfull');
                    next();
                });
        },
        /*
         * After a moray bucket is migrated to a version that adds a new index,
         * it is important to make sure that it's safe to use for both read and
         * write operations. For instance, search filters will not work as
         * expected when a bucket is being reindexed and putobject operations
         * will also not use the updated bucket schema if they write to a row
         * that hasn't been reindexed yet, leading to data corruption.
         *
         * To check that a bucket has been properly reindexed after an update,
         * we need to check that:
         *
         * 1. The migrated bucket is at the expected version.
         *
         * 2. The 'reindex_active' column of the row representing the migrated
         * bucket in the 'buckets_config'' table has a value representing an
         * empty object.
         *
         * 3. All rows in the table storing the migrated bucket's data' have the
         * expected version number.
         */
        function checkBucketsAtExpectedVersion(_, next) {
            var expectedResults = options.expectedResults;

            vasync.forEachPipeline({
                func: function checkBucketVersion(expectedResult, done) {
                    assert.object(expectedResult, 'expectedResult');

                    var bucketName = expectedResult.bucketName;
                    assert.string(bucketName, 'bucketName');

                    var expectedVersion = expectedResult.version;
                    assert.number(expectedVersion, 'expectedVersion');

                    morayClient.getBucket(bucketName,
                        function onGetBucket(getBucketErr, bucket) {
                            t.ifError(getBucketErr,
                                'getting bucket should not error');
                            t.ok(bucket, 'bucket should be present');

                            if (bucket) {
                                t.equal(bucket.options.version, expectedVersion,
                                    'Bucket with name ' + bucketName +
                                        ' should be at version ' +
                                        expectedVersion);
                            }

                            done();
                        });
                },
                inputs: expectedResults
            }, next);
        },
        function checkObjectsAtExpectedVersion(_, next) {
            var expectedResults = options.expectedResults;

            vasync.forEachPipeline({
                func: function checkObjectsVersion(expectedResult, done) {
                    assert.object(expectedResult, 'expectedResult');

                    var bucketName = expectedResult.bucketName;
                    assert.string(bucketName, 'bucketName');

                    var expectedVersion = expectedResult.version;
                    assert.number(expectedVersion, 'expectedVersion');

                    getAllObjectsRowVer(morayClient, bucketName,
                        function onGetAllObjects(getRowsVerErr, allRecords) {
                            var allRecordsAtExpectedVersion = false;

                            t.ifError(getRowsVerErr,
                                'getting all rows versions should not error');
                            t.ok(allRecords,
                                'list of records should be present');

                            if (!allRecords) {
                                done();
                                return;
                            }

                            t.strictEqual(allRecords.length, NB_TEST_OBJECTS,
                                NB_TEST_OBJECTS + ' records must have ' +
                                    'been checked');

                            allRecordsAtExpectedVersion =
                                allRecords.every(function checkVersion(record) {
                                    assert.object(record, 'record');

                                    return record._rver === expectedVersion;
                                });

                            t.ok(allRecordsAtExpectedVersion,
                                'all records should be at version ' +
                                    expectedVersion.version);

                            done();
                        });
                },
                inputs: expectedResults
            }, function allVersionsChecked(err) {
                next(err);
            });
        },
        function checkNoBucketHasReindexingActive(_, next) {
            var expectedResults = options.expectedResults;

            vasync.forEachPipeline({
                func: function checkNoReindexingActive(expectedResult, done) {
                    var bucketName = expectedResult.bucketName;
                    assert.string(bucketName, 'bucketName');

                    morayClient.getBucket(bucketName,
                        function onGetVmBucket(getBucketErr, bucket) {
                            var reindexActive =
                                bucket.reindex_active !== undefined &&
                                    Object.keys(bucket.reindex_active).length >
                                        0;

                            t.ok(!getBucketErr, 'Getting bucket ' + bucketName +
                                ' should not error');
                            t.ok(!reindexActive, 'bucket ' + bucketName +
                                ' should not be reindexing');

                            done();
                        });
                },
                inputs: expectedResults
            }, next);
        },
        function checkMorayBucketsInitStatus(_, next) {
            var bucketsInitStatus = morayBucketsInitializer.status();
            var expectedBucketsInitStatus = {
                bucketsSetup: {state: 'DONE'},
                bucketsReindex: {state: 'DONE'},
                /*
                 * No data migrations path was passed to the
                 * MorayBucketsInitializer constructor above, so no data
                 * migration will be started.
                 */
                dataMigrations: {state: 'NOT_STARTED'}
            };

            t.deepEqual(bucketsInitStatus, expectedBucketsInitStatus);

            next();
        }
    ]}, function allMigrationTestsDone(migrationTestsErr) {
        t.ok(!migrationTestsErr, 'migration test should not error');

        if (morayClient) {
            morayClient.close();
        }

        callback();
    });
}

test('Moray buckets schema migrations', function (t) {
    var morayBucketsInitializer;
    var morayClient;

    var TEST_BUCKET_NAME = 'moray_buckets_test_schema_migrations';

    /*
     * Initial buckets configuration, version 0.
     */
    var TEST_BUCKET_CONFIG_V0 = {
        name: TEST_BUCKET_NAME,
        schema: {
            index: {
                foo: { type: 'string' }
            }
        }
    };

    /*
     * Buckets configuration at version 1: an index is added on the property
     * named "indexed_property". The upgrade from version 0 to version 1 is
     * valid.
     */
    var TEST_BUCKET_CONFIG_V1 = {
        name: TEST_BUCKET_NAME,
        schema: {
            index: {
                foo: { type: 'string' },
                indexed_property: { type: 'string' }
            },
            options: {
                version: 1
            }
        }
    };

    /*
     * Buckets configuration at version 2: an index is added on the property
     * named "another_indexed_property". The upgrade from version 1 to version 2
     * is valid.
     */
    var TEST_BUCKET_CONFIG_V2 = {
        name: TEST_BUCKET_NAME,
        schema: {
            index: {
                foo: { type: 'string' },
                indexed_property: { type: 'string' },
                another_indexed_property: { type: 'string' }
            },
            options: {
                version: 2
            }
        }
    };

    var testBucketsConfigV0 = {
        test_model: TEST_BUCKET_CONFIG_V0
    };

    var testBucketsConfigV1 = {
        vms: TEST_BUCKET_CONFIG_V1
    };

    var testBucketsConfigV2 = {
        vms: TEST_BUCKET_CONFIG_V2
    };

    vasync.pipeline({funcs: [
        function connectToMoray(_, next) {
            morayClient = testMoray.creatTestMorayClient({
                log: TEST_LOGGER
            });

            morayClient.once('connect', next);
            morayClient.once('error', next);
        },
        function cleanup(_, next) {
            morayClient.delBucket(TEST_BUCKET_NAME,
                function onDel(delBucketErr) {
                    if (delBucketErr &&
                        verror.hasCauseWithName(delBucketErr,
                            'BucketNotFoundError')) {
                        next();
                    } else {
                        next(delBucketErr);
                    }
                });
        },
        function setupOriginalMorayBuckets(_, next) {
            morayBucketsInitializer  = new MorayBucketsInitializer({
                bucketsConfig: testBucketsConfigV0,
                log: TEST_LOGGER,
                morayClient: morayClient
            });

            morayBucketsInitializer.start();

            function cleanUp() {
                morayBucketsInitializer.removeAllListeners('error');
                morayBucketsInitializer.removeAllListeners('done');
            }

            morayBucketsInitializer.once('done',
                function onMorayBucketsInit() {
                    t.ok(true,
                        'original moray buckets setup should be ' +
                            'successful');

                    cleanUp();
                    next();
                });

            morayBucketsInitializer.once('error',
                function onMorayBucketsInitError(morayBucketsInitErr) {
                    t.ok(!morayBucketsInitErr,
                        'original moray buckets initialization should ' +
                            'not error');

                    cleanUp();
                    next(morayBucketsInitErr);
                });
        },
        function writeTestObjects(_, next) {
            testMoray.writeObjects(morayClient, TEST_BUCKET_NAME, {
                indexed_property: 'foo'
            }, NB_TEST_OBJECTS, function onTestObjectsWritten(err) {
                t.ok(!err, 'writing test objects should not error');
                morayClient.close();
                next(err);
            });
        },
        /*
         * First, migrate from version 0 to 1, which is a valid migration and
         * results in the bucket storing VM objects to be at version 1.
         */
        function migrateFromV0ToV1(_, next) {
            testMigrationToBucketsConfig(testBucketsConfigV1, {
                expectedResults: [
                    {
                        bucketName: TEST_BUCKET_NAME,
                        version: 1
                    }
                ]
            }, t, next);
        },
        /*
         * Then, attempt to migrate from version 1 to 0 (a downgrade), which is
         * a valid migration but results in the bucket storing VM objects to
         * stay at version 1.
         */
        function migrateFromV1ToV0(_, next) {
            testMigrationToBucketsConfig(testBucketsConfigV0, {
                expectedResults: [
                    {
                        bucketName: TEST_BUCKET_NAME,
                        version: 1
                    }
                ]
            }, t, next);
        },
        /*
         * Finally, migrate from version 1 to 2, which is a valid migration and
         * results in the bucket storing VM objects to be at version 2.
         */
        function migrateFromV1ToV2(_, next) {
            testMigrationToBucketsConfig(testBucketsConfigV2, {
                expectedResults: [
                    {
                        bucketName: TEST_BUCKET_NAME,
                        version: 2
                    }
                ]
            }, t, next);
        }
    ]}, function allMigrationsDone(allMigrationsErr) {
        t.ok(!allMigrationsErr, 'versioning test should not error');
        t.end();
    });
});

test('Backward incompatible Moray buckets schema migration', function (t) {
    var morayBucketsInitializer;
    var morayClient;

    var TEST_BUCKET_NAME = 'moray_buckets_test_schema_migrations_index_removal';

    /*
     * Initial buckets configuration, version 0.
     */
    var TEST_BUCKET_CONFIG_V0 = {
        name: TEST_BUCKET_NAME,
        schema: {
            index: {
                foo: { type: 'string' },
                bar: { type: 'string' }
            }
        }
    };

    /*
     * Buckets configuration at version 1: an index is *removed*. The upgrade
     * from version 0 to version 1 is thus not backward compatible and is
     * invalid.
     */
    var TEST_BUCKET_CONFIG_V1 = {
        name: TEST_BUCKET_NAME,
        schema: {
            index: {
                foo: { type: 'string' }
            },
            options: {
                version: 1
            }
        }
    };

    var testBucketsConfigV0 = {
        test_model: TEST_BUCKET_CONFIG_V0
    };

    var testBucketsConfigV1 = {
        vms: TEST_BUCKET_CONFIG_V1
    };

    vasync.pipeline({funcs: [
        function connectToMoray(_, next) {
            morayClient = testMoray.creatTestMorayClient({
                log: TEST_LOGGER
            });

            morayClient.once('connect', next);
            morayClient.once('error', next);
        },
        function cleanup(_, next) {
            morayClient.delBucket(TEST_BUCKET_NAME,
                function onDel(delBucketErr) {
                    if (delBucketErr &&
                        verror.hasCauseWithName(delBucketErr,
                            'BucketNotFoundError')) {
                        next();
                    } else {
                        next(delBucketErr);
                    }
                });
        },
        function setupOriginalMorayBuckets(_, next) {
            morayBucketsInitializer  = new MorayBucketsInitializer({
                bucketsConfig: testBucketsConfigV0,
                log: TEST_LOGGER,
                morayClient: morayClient
            });

            morayBucketsInitializer.start();

            function cleanUp() {
                morayBucketsInitializer.removeAllListeners('error');
                morayBucketsInitializer.removeAllListeners('done');
            }

            morayBucketsInitializer.once('done',
                function onMorayBucketsInit() {
                    t.ok(true,
                        'original moray buckets setup should be ' +
                            'successful');

                    cleanUp();
                    next();
                });

            morayBucketsInitializer.once('error',
                function onMorayBucketsInitError(morayBucketsInitErr) {
                    t.ok(!morayBucketsInitErr,
                        'original moray buckets initialization should ' +
                            'not error');

                    cleanUp();
                    next(morayBucketsInitErr);
                });
        },
        function performInvalidSchemaMigration(_, next) {
            morayBucketsInitializer  = new MorayBucketsInitializer({
                bucketsConfig: testBucketsConfigV1,
                log: TEST_LOGGER,
                morayClient: morayClient
            });

            morayBucketsInitializer.start();

            function cleanUp() {
                morayBucketsInitializer.removeAllListeners('error');
                morayBucketsInitializer.removeAllListeners('done');
            }

            morayBucketsInitializer.once('done',
                function onMorayBucketsInit() {
                    t.ok(false, 'non backward-compatible schema migration ' +
                        'should not be successful');

                    cleanUp();
                    next();
                });

            morayBucketsInitializer.once('error',
                function onMorayBucketsInitError(morayBucketsInitErr) {
                    t.ok(morayBucketsInitErr,
                        'non backward-compatible schema migration should ' +
                            'error');

                    cleanUp();
                    next();
                });
        }
    ]}, function allTestsDone(testsErr) {
        t.ifError(testsErr, 'tests should not error');

        if (morayClient) {
            morayClient.close();
        }

        t.end();
    });
});
