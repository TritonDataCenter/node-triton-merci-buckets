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
var path = require('path');
var test = require('tape');
var util = require('util');
var vasync = require('vasync');
var verror = require('verror');

var MorayBucketsInitializer = require('../index').MorayBucketsInitializer;

var testMoray = require('./lib/moray');

var TEST_BUCKET_NAME = 'moray_buckets_test_data_migrations';
var TEST_BUCKETS_CONFIG_NO_DATA_MIGRATIONS = {};
var TEST_BUCKETS_CONFIG_WITH_DATA_MIGRATIONS = {};
var TEST_LOGGER = bunyan.createLogger({
    name: 'test-data-migrations'
});
var TEST_MODEL_NAME = 'test_model';

/*
 * We use two versions for the VMS_BUCKET_CONFIG (VMS_BUCKET_CONFIG_V1 and
 * VMS_BUCKET_CONFIG_V2) to exercise the code path where finding objects to
 * migrate fails with an InvalidQueryError due to the fact that some Moray
 * instances do not have the data_version field indexed in their bucket cache.
 * See https://smartos.org/bugview/TRITON-214 for context.
 */
TEST_BUCKETS_CONFIG_NO_DATA_MIGRATIONS[TEST_MODEL_NAME] = {
    name: TEST_BUCKET_NAME,
    schema: {
        index: {
            foo: { type: 'string' },
            bar: { type: 'string' }
        }
    }
};

TEST_BUCKETS_CONFIG_WITH_DATA_MIGRATIONS[TEST_MODEL_NAME] = {
    name: TEST_BUCKET_NAME,
    schema: {
        index: {
            foo: { type: 'string' },
            bar: { type: 'string' },
            data_version: { type: 'number' }
        },
        options: {
            version: 1
        }
    }
};

/*
 * The number of test objects is chosen so that it's larger than the default
 * page for Moray requests (which is currently 1000). 2001 objects means that at
 * least 3 Moray requests are necessary to read all records from the test moray
 * buckets, and so that we go through 3 iterations of the read/transform/write
 * cycle involved in migrating records.
 */
var NUM_TEST_OBJECTS = 2001;

function findAllObjects(morayClient, bucketName, filter, callback) {
    assert.object(morayClient, 'morayClient');
    assert.string(bucketName, 'bucketName');
    assert.func(callback, 'callback');

    var allRecords = [];

    var findAllObjectsReq = morayClient.findObjects(bucketName, filter);

    findAllObjectsReq.once('error', function onError(findErr) {
        cleanup();
        callback(findErr);
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

test('data migrations with invalid filenames', function (t) {
    var morayBucketsInitializer;
    var morayClient = testMoray.creatTestMorayClient({
        log: TEST_LOGGER
    });

    morayBucketsInitializer  = new MorayBucketsInitializer({
        bucketsConfig: TEST_BUCKETS_CONFIG_WITH_DATA_MIGRATIONS,
        dataMigrationsPath: path.resolve(__dirname, 'fixtures',
            'test-data-migrations', 'data-migrations-invalid-filenames'),
        log: TEST_LOGGER,
        morayClient: morayClient
    });

    morayBucketsInitializer.start();

    morayBucketsInitializer.once('error', function onInitError(initErr) {
        var expectedErrorName = 'InvalidDataMigrationFileNamesError';

        t.ok(initErr,
            'loading migrations with invalid filenames should error');
        if (initErr) {
            t.ok(verror.hasCauseWithName(initErr, expectedErrorName),
                'error should have a cause of ' + expectedErrorName);
        }

        morayClient.close();

        t.end();
    });

    morayBucketsInitializer.once('done', function onInitDone() {
        t.ok(false, 'buckets init should not be successful');

        morayClient.close();

        t.end();
    });
});

test('data migrations with transient error', function (t) {
    var context = {};
    var morayBucketsInitializer;
    var TRANSIENT_ERROR_MSG = 'Mocked transient error';

    vasync.pipeline({arg: context, funcs: [
        function connectToMoray(ctx, next) {
            ctx.morayClient = testMoray.creatTestMorayClient({
                log: TEST_LOGGER
            });

            ctx.morayClient.once('connect', next);
            ctx.morayClient.once('error', next);
        },
        function cleanup(ctx, next) {
            ctx.morayClient.delBucket(TEST_BUCKET_NAME,
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
        function setupMorayBuckets(ctx, next) {
            morayBucketsInitializer = new MorayBucketsInitializer({
                bucketsConfig: TEST_BUCKETS_CONFIG_NO_DATA_MIGRATIONS,
                log: TEST_LOGGER,
                morayClient: ctx.morayClient
            });

            morayBucketsInitializer.start();

            morayBucketsInitializer.once('done', function bucketsInitDone() {
                t.pass('buckets init should succeed');
                next();
            });

            morayBucketsInitializer.once('error',
                function bucketsInitError(bucketsInitErr) {
                    t.ifError(bucketsInitErr, 'buckets init should not error');
                    next(bucketsInitErr);
                });
        },
        function checkMorayBucketsInitStatus(_, next) {
            var bucketsReindexStatus =
                morayBucketsInitializer.status().bucketsReindex;
            var bucketsSetupStatus =
                morayBucketsInitializer.status().bucketsSetup;
            var expectedBucketsReindexState = 'DONE';
            var expectedBucketsSetupState = 'DONE';

            t.ifError(bucketsReindexStatus.latestError,
                'buckets reindex status should not have latest error present');
            t.equal(bucketsReindexStatus.state, expectedBucketsReindexState,
                'buckets reindex state should be ' +
                    expectedBucketsReindexState + ' got: ' +
                    bucketsReindexStatus.state);

            t.ifError(bucketsSetupStatus.latestError,
                'buckets setup status should not have latest error present');
            t.equal(bucketsSetupStatus.state, expectedBucketsSetupState,
                'buckets setup state should be ' +
                    expectedBucketsSetupState + ' got: ' +
                    bucketsSetupStatus.state);

            next();
        },
        function writeTestObjects(ctx, next) {
            assert.object(ctx.morayClient, 'ctx.morayClient');

            testMoray.writeObjects(ctx.morayClient, TEST_BUCKET_NAME, {
                foo: 'foo'
            }, NUM_TEST_OBJECTS, function onTestObjectsWritten(writeErr) {
                t.ok(!writeErr, 'writing test objects should not error, got: ' +
                    util.inspect(writeErr));
                next(writeErr);
            });
        },
        function migrateSchemaForDataMigrations(ctx, next) {
            morayBucketsInitializer = new MorayBucketsInitializer({
                bucketsConfig: TEST_BUCKETS_CONFIG_WITH_DATA_MIGRATIONS,
                log: TEST_LOGGER,
                morayClient: ctx.morayClient
            });

            morayBucketsInitializer.start();

            morayBucketsInitializer.once('done', function bucketsInitDone() {
                t.pass('buckets init after schema upgrade should succeed');
                next();
            });

            morayBucketsInitializer.once('error',
                function bucketsInitError(bucketsInitErr) {
                    t.ifError(bucketsInitErr,
                        'buckets init after schema upgrade should not error');
                    next(bucketsInitErr);
                });
        },
        function injectTransientError(ctx, next) {
            ctx.originalBatch = ctx.morayClient.batch;
            ctx.morayClient.batch =
                function mockedBatch(opsList, callback) {
                    assert.arrayOfObject(opsList, 'opsList');
                    assert.func(callback, 'callback');

                    callback(new Error(TRANSIENT_ERROR_MSG));
                };
            next();
        },
        function startMigrations(ctx, next) {
            morayBucketsInitializer = new MorayBucketsInitializer({
                bucketsConfig: TEST_BUCKETS_CONFIG_WITH_DATA_MIGRATIONS,
                log: TEST_LOGGER,
                morayClient: ctx.morayClient,
                dataMigrationsPath: path.join(__dirname, 'fixtures',
                    'test-data-migrations', 'data-migrations-valid')
            });

            morayBucketsInitializer.start();

            morayBucketsInitializer.once('done', function onBucketsInitDone() {
                t.fail('moray buckets init should not complete while ' +
                    'transient error injected');
            });

            morayBucketsInitializer.once('error',
                function onBucketsInitError(bucketsInitErr) {
                    t.fail('moray buckets init should not error while ' +
                        'transient error injected, got: ' + bucketsInitErr);
                });

            next();
        },
        function checkDataMigrationsTransientError(_, next) {
            var MAX_NUM_TRIES;
            /*
             * We wait for the moray bucket cache to be refreshed on all Moray
             * instances, which can be up to 5 minutes currently, and then some.
             * This is the maximum delay during which InvalidQueryError can
             * occur due to stale buckets cache, after which only the transient
             * error injected by this test should surface.
             */
            var MAX_TRIES_DURATION_IN_MS = 6 * 60 * 1000;
            var NUM_TRIES = 0;
            var RETRY_DELAY_IN_MS = 10000;

            MAX_NUM_TRIES = MAX_TRIES_DURATION_IN_MS / RETRY_DELAY_IN_MS;

            function doCheckMigrationsStatus() {
                var bucketsInitStatus;
                var dataMigrationsStatus;
                var errMsg;
                var foundExpectedErrMsg;
                var latestMigrationsErrs;

                ++NUM_TRIES;

                bucketsInitStatus = morayBucketsInitializer.status();
                t.ok(bucketsInitStatus, 'buckets init status must be present');

                if (bucketsInitStatus) {
                    t.ok(bucketsInitStatus.dataMigrations,
                        'data migrations status must be present');
                    dataMigrationsStatus = bucketsInitStatus.dataMigrations;
                    latestMigrationsErrs = dataMigrationsStatus.latestErrors;
                }

                if (latestMigrationsErrs &&
                    latestMigrationsErrs[TEST_MODEL_NAME]) {
                    errMsg = latestMigrationsErrs[TEST_MODEL_NAME].message;
                    foundExpectedErrMsg =
                        errMsg.indexOf(TRANSIENT_ERROR_MSG) !== -1;

                    t.ok(foundExpectedErrMsg,
                            'data migrations latest error should include ' +
                                TRANSIENT_ERROR_MSG + ', got: ' +
                                latestMigrationsErrs[TEST_MODEL_NAME].message);
                        next();
                } else {
                    if (NUM_TRIES >= MAX_NUM_TRIES) {
                        t.ok(false, 'max number of tries exceeded');
                        next();
                    } else {
                        setTimeout(doCheckMigrationsStatus,
                            RETRY_DELAY_IN_MS);
                    }
                }
            }

            doCheckMigrationsStatus();
        },
        function removeTransientError(ctx, next) {
            morayBucketsInitializer.removeAllListeners('done');
            morayBucketsInitializer.removeAllListeners('error');

            ctx.morayClient.batch = ctx.originalBatch;

            morayBucketsInitializer.once('done',
                function onMorayBucketsInitDone() {
                    t.ok(true,
                        'moray buckets init should eventually complete ' +
                            'successfully');
                    next();
                });

            morayBucketsInitializer.once('error',
                function onMorayBucketsInitError(bucketsInitErr) {
                    t.ok(false, 'moray buckets init should not error, got: ' +
                        util.inspect(bucketsInitErr));
                    next(bucketsInitErr);
                });
        },
        function readTestObjects(ctx, next) {
            assert.object(ctx.morayClient, 'ctx.morayClient');

            findAllObjects(ctx.morayClient, TEST_BUCKET_NAME, '(foo=*)',
                function onFindAllObjects(findErr, objects) {
                    var nonMigratedObjects;

                    t.ok(!findErr,
                        'reading all objects back should not error, got: ' +
                            util.inspect(findErr));
                    t.ok(objects,
                        'reading all objects should not return empty response');

                    if (objects) {
                        nonMigratedObjects =
                            objects.filter(function checkObjects(object) {
                                return object.value.bar !== 'foo';
                            });
                        t.equal(nonMigratedObjects.length, 0,
                            'data migrations should have migrated all objects' +
                                ', got the following non-migrated objects: ' +
                                nonMigratedObjects.join(', '));
                    }

                    next(findErr);
                });
        },
        function checkDataMigrationsDone(_, next) {
            var dataMigrationsStatus =
                morayBucketsInitializer.status().dataMigrations;
            var latestExpectedCompletedVmsMigration = 1;
            var latestCompletedMigrations;

            if (dataMigrationsStatus && dataMigrationsStatus.completed) {
                latestCompletedMigrations =
                    dataMigrationsStatus.completed[TEST_MODEL_NAME];
            }

            t.equal(latestExpectedCompletedVmsMigration,
                latestCompletedMigrations,
                'latest completed data migration for vms model should be ' +
                    'at version ' + latestExpectedCompletedVmsMigration);

            next();
        }
    ]}, function allMigrationsDone(allMigrationsErr) {
        t.ok(!allMigrationsErr, 'data migrations test should not error');

        if (context.morayClient) {
            context.morayClient.close();
        }

        t.end();
    });
});

test('data migrations with non-transient error', function (t) {
    var context = {};
    var morayBucketsInitializer;

    vasync.pipeline({arg: context, funcs: [
        function connectToMoray(ctx, next) {
            ctx.morayClient = testMoray.creatTestMorayClient({
                log: TEST_LOGGER
            });

            ctx.morayClient.once('connect', next);
            ctx.morayClient.once('error', next);
        },
        function cleanup(ctx, next) {
            ctx.morayClient.delBucket(TEST_BUCKET_NAME,
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
        function setupMorayBuckets(ctx, next) {
            morayBucketsInitializer = new MorayBucketsInitializer({
                bucketsConfig: TEST_BUCKETS_CONFIG_WITH_DATA_MIGRATIONS,
                log: TEST_LOGGER,
                morayClient: ctx.morayClient
            });

            morayBucketsInitializer.start();

            morayBucketsInitializer.once('done', next);
            morayBucketsInitializer.once('error', next);
        },
        function writeTestObjects(ctx, next) {
            assert.object(ctx.morayClient, 'ctx.morayClient');

            testMoray.writeObjects(ctx.morayClient, TEST_BUCKET_NAME, {
                foo: 'foo'
            }, NUM_TEST_OBJECTS, function onTestObjectsWritten(writeErr) {
                t.ok(!writeErr, 'writing test objects should not error, got: ' +
                    util.inspect(writeErr));
                next(writeErr);
            });
        },
        function injectNonTransientError(ctx, next) {
            ctx.originalBatch = ctx.morayClient.batch;
            ctx.morayClient.batch =
                function mockedBatch(opsList, callback) {
                    assert.arrayOfObject(opsList, 'opsList');
                    assert.func(callback, 'callback');

                    callback(new verror.VError({
                        name: 'BucketNotFoundError'
                    }, 'non-transient error'));
                };
            next();
        },
        function startMigrations(ctx, next) {
            morayBucketsInitializer = new MorayBucketsInitializer({
                bucketsConfig: TEST_BUCKETS_CONFIG_WITH_DATA_MIGRATIONS,
                log: TEST_LOGGER,
                morayClient: ctx.morayClient,
                dataMigrationsPath: path.join(__dirname, 'fixtures',
                    'test-data-migrations', 'data-migrations-valid')
            });

            morayBucketsInitializer.start();

            morayBucketsInitializer.once('done',
                function onBucketsInitDone() {
                    t.ok(false, 'buckets init should not complete when ' +
                        'non-transient error injected');
                    next();
                });

            morayBucketsInitializer.once('error',
                function onBucketsInitError(bucketsInitErr) {
                    t.ok(true, 'Moray buckets init should error when ' +
                        'non-transient error injected, got: ' +
                        bucketsInitErr.toString());
                    next();
                });
        }
    ]}, function allMigrationsDone(allMigrationsErr) {
        t.ok(!allMigrationsErr, 'data migrations test should not error');

        if (context.morayClient) {
            context.morayClient.close();
        }

        t.end();
    });
});
