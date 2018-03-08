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

var DataMigrationsController =
    require('../lib/data-migrations/controller').DataMigrationsController;
var dataMigrationsLoader = require('../lib/data-migrations/loader');
var MorayBucketsInitializer = require('../index').MorayBucketsInitializer;

var testMoray = require('./lib/moray');

var TEST_BUCKET_NAME = 'moray_buckets_test_data_migrations';
var TEST_BUCKETS_CONFIG = {};
var TEST_LOGGER = bunyan.createLogger({
    name: 'test-data-migrations'
});
var TEST_MODEL_NAME = 'test_model';

TEST_BUCKETS_CONFIG[TEST_MODEL_NAME] = {
    name: TEST_BUCKET_NAME,
    schema: {
        index: {
            foo: { type: 'string' },
            bar: { type: 'string' },
            data_version: { type: 'number' }
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
    var dataMigrationsLoaderLogger = bunyan.createLogger({
        name: 'data-migrations-loader',
        level: 'debug'
    });

    dataMigrationsLoader.loadMigrations({
        log: dataMigrationsLoaderLogger,
        migrationsRootPath: path.resolve(__dirname, 'fixtures',
            'test-data-migrations', 'data-migrations-invalid-filenames')
    }, function onMigrationsLoaded(loadMigrationsErr/* , migrations */) {
        var expectedErrorName = 'InvalidDataMigrationFileNamesError';

        t.ok(loadMigrationsErr,
            'loading migrations with invalid filenames should error');

        if (loadMigrationsErr) {
            t.ok(verror.hasCauseWithName(loadMigrationsErr, expectedErrorName),
                'error should have a cause of ' + expectedErrorName);
        }

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
                bucketsConfig: TEST_BUCKETS_CONFIG,
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
        function loadDataMigrations(ctx, next) {
            var dataMigrationsLoaderLogger = bunyan.createLogger({
                name: 'data-migrations-loader',
                level: 'info'
            });

            dataMigrationsLoader.loadMigrations({
                log: dataMigrationsLoaderLogger,
                migrationsRootPath: path.resolve(__dirname, 'fixtures',
                    'test-data-migrations', 'data-migrations-valid')
            }, function onMigrationsLoaded(loadMigrationsErr, migrations) {
                ctx.migrations = migrations;
                next(loadMigrationsErr);
            });
        },
        function createMigrationsController(ctx, next) {
            assert.object(ctx.migrations, 'ctx.migrations');
            assert.object(ctx.morayClient, 'ctx.morayClient');

            ctx.dataMigrationsCtrl = new DataMigrationsController({
                bucketsConfig: TEST_BUCKETS_CONFIG,
                log: bunyan.createLogger({
                    name: 'data-migratons-controller',
                    level: 'info'
                }),
                migrations: ctx.migrations,
                morayClient: ctx.morayClient
            });

            next();
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
            ctx.dataMigrationsCtrl.runMigrations();

            ctx.dataMigrationsCtrl.once('done',
                function onDataMigrationsDone() {
                    t.ok(false, 'data migrations should not complete when ' +
                        'transient error injected');
                });

            ctx.dataMigrationsCtrl.once('error',
                function onDataMigrationsError(/* dataMigrationsErr */) {
                    t.ok(false, 'data migrations should not error when ' +
                        'transient error injected');
                });

                next();
        },
        function checkDataMigrationsTransientError(ctx, next) {
            var MAX_NUM_TRIES = 20;
            var NUM_TRIES = 0;
            var RETRY_DELAY_IN_MS = 1000;

            assert.object(ctx.vmapiClient, 'ctx.vmapiClient');

            function doCheckMigrationsStatus() {
                var foundExpectedErrMsg;
                var latestMigrationsErr;

                ++NUM_TRIES;

                latestMigrationsErr =
                    ctx.dataMigrationsCtrl.getLatestErrors(TEST_MODEL_NAME);
                if (latestMigrationsErr) {
                    foundExpectedErrMsg =
                        latestMigrationsErr.indexOf(TRANSIENT_ERROR_MSG) !== -1;
                    t.ok(foundExpectedErrMsg,
                            'data migrations latest error should include ' +
                                TRANSIENT_ERROR_MSG + ', got: ' +
                                latestMigrationsErr);
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
            ctx.dataMigrationsCtrl.removeAllListeners('done');
            ctx.dataMigrationsCtrl.removeAllListeners('error');

            ctx.morayClient.batch = ctx.originalBatch;

            ctx.dataMigrationsCtrl.once('done',
                function onDataMigrationsDone() {
                    t.ok(true,
                        'data migration should eventually complete ' +
                            'successfully');
                    next();
                });

            ctx.dataMigrationsCtrl.once('error',
                function onDataMigrationsError(dataMigrationErr) {
                    t.ok(false, 'data migrations should not error, got: ' +
                        util.inspect(dataMigrationErr));
                    next(dataMigrationErr);
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
        function checkDataMigrationsDone(ctx, next) {
            var latestExpectedCompletedVmsMigration = 1;
            var latestCompletedMigrations =
                ctx.dataMigrationsCtrl.getLatestCompletedMigrationForModel(
                    TEST_MODEL_NAME);

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

/*
exports.data_migrations_non_transient_error = function (t) {
    var context = {};

    vasync.pipeline({arg: context, funcs: [
        function cleanup(ctx, next) {
            testMoray.cleanupLeftoverBuckets([
                VMS_BUCKET_NAME,
                SERVER_VMS_BUCKET_NAME,
                ROLE_TAGS_BUCKET_NAME
            ],
            function onCleanupLeftoverBuckets(cleanupErr) {
                t.ok(!cleanupErr,
                    'cleaning up leftover buckets should be successful');
                next(cleanupErr);
            });
        },
        function setupMorayBuckets(ctx, next) {
            var morayBucketsInitializer;
            var morayClient;
            var moraySetup = morayInit.startMorayInit({
                morayConfig: common.config.moray,
                morayBucketsConfig: TEST_BUCKETS_CONFIG,
                changefeedPublisher: changefeedUtils.createNoopCfPublisher()
            });

            ctx.moray = moraySetup.moray;
            ctx.morayBucketsInitializer = morayBucketsInitializer =
                moraySetup.morayBucketsInitializer;
            ctx.morayClient = morayClient = moraySetup.morayClient;

            function cleanUp() {
                morayBucketsInitializer.removeAllListeners('error');
                morayBucketsInitializer.removeAllListeners('done');
            }

            morayBucketsInitializer.on('done', function onMorayBucketsInit() {
                t.ok(true,
                    'original moray buckets setup should be ' +
                        'successful');

                cleanUp();
                next();
            });

            morayBucketsInitializer.on('error',
                function onMorayBucketsInitError(morayBucketsInitErr) {
                    t.ok(!morayBucketsInitErr,
                        'original moray buckets initialization should ' +
                            'not error');

                    cleanUp();
                    next(morayBucketsInitErr);
                });
        },
        function writeTestObjects(ctx, next) {
            assert.object(ctx.morayClient, 'ctx.morayClient');

            writeObjects(ctx.morayClient, VMS_BUCKET_NAME, {
                foo: 'foo'
            }, NUM_TEST_OBJECTS, function onTestObjectsWritten(writeErr) {
                t.ok(!writeErr, 'writing test objects should not error, got: ' +
                    util.inspect(writeErr));
                next(writeErr);
            });
        },
        function loadDataMigrations(ctx, next) {
            var dataMigrationsLoaderLogger = bunyan.createLogger({
                name: 'data-migrations-loader',
                level: 'info'
            });

            dataMigrationsLoader.loadMigrations({
                log: dataMigrationsLoaderLogger,
                migrationsRootPath: path.resolve(__dirname, 'fixtures',
                    'data-migrations-valid')
            }, function onMigrationsLoaded(loadMigrationsErr, migrations) {
                ctx.migrations = migrations;
                next(loadMigrationsErr);
            });
        },
        function injectNonTransientError(ctx, next) {
            ctx.originalPutBatch = ctx.moray.putBatch;
            ctx.moray.putBatch =
                function mockedPutBatch(modelName, records, callback) {
                    assert.string(modelName, 'modelName');
                    assert.arrayOfObject(records, 'records');
                    assert.func(callback, 'callback');

                    callback(new VError({
                        name: 'BucketNotFoundError'
                    }, 'non-transient error'));
                };
            next();
        },
        function startMigrations(ctx, next) {
            assert.object(ctx.migrations, 'ctx.migrations');
            assert.object(ctx.moray, 'ctx.moray');

            ctx.dataMigrationsCtrl = new DataMigrationsController({
                log: bunyan.createLogger({
                    name: 'data-migratons-controller',
                    level: 'info'
                }),
                migrations: ctx.migrations,
                moray: ctx.moray
            });

            ctx.dataMigrationsCtrl.start();

            ctx.dataMigrationsCtrl.once('done',
                function onDataMigrationsDone() {
                    t.ok(false, 'data migration should not complete when ' +
                        'non-transient error injected');
                });

            ctx.dataMigrationsCtrl.once('error',
                function onDataMigrationsError(dataMigrationErr) {
                    t.ok(true, 'data migrations should error when ' +
                        'non-transient error injected, got: ' +
                        dataMigrationErr.toString());
                    next();
                });
        }
    ]}, function allMigrationsDone(allMigrationsErr) {
        t.equal(allMigrationsErr, undefined,
                'data migrations test should not error');
        context.morayClient.close();
        t.done();
    });
};
*/
