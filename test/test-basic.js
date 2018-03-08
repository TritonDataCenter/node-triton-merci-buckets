/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var bunyan = require('bunyan');
var jsprim = require('jsprim');
var libuuid = require('libuuid');
var morayBuckets = require('../index');
var path = require('path');
var testMoray = require('./lib/moray');
var vasync = require('vasync');
var verror = require('verror');

var TEST_BUCKET_NAME = 'test_bucket_migrations';
var TEST_LOGGER = bunyan.createLogger({
    name: 'test-basic'
});

var bucketsConfig = {
    foo: {
        name: TEST_BUCKET_NAME,
        schema: {
            index: {
                bar: {type: 'string'},
                /*
                 * This field would be required to support data migrations.
                 */
                data_version: {type: 'number'}
            },
            options: {
                version: 2
            }
        }
    }
};

var bucketsInitializer;
var dataMigrations;
var dataMigrationsDirPath = path.join(__dirname,
    './fixtures/basic-test/data-migrations');
var morayClient;

vasync.pipeline({funcs: [
    function connectToMoray(_, next) {
        console.log('connecting to moray...');

        morayClient = testMoray.creatTestMorayClient({
            log: TEST_LOGGER
        });

        morayClient.once('connect', next);
        morayClient.on('error', next);
    },
    function deletePreviousTestBucket(_, next) {
        console.log('deleting test bucket');
        morayClient.delBucket(TEST_BUCKET_NAME, function onDel(delBucketErr) {
            if (delBucketErr &&
                verror.hasCauseWithName(delBucketErr, 'BucketNotFoundError')) {
                next();
            } else {
                next(delBucketErr);
            }
        });
    },
    function createTestBucket(_, next) {
        console.log('creating test bucket');
        morayClient.putBucket(TEST_BUCKET_NAME, bucketsConfig.foo, next);
    },
    function createTestObjects(_, next) {
        var i;
        var objectKeys = [];
        var nbObjects = 2001;
        var valueTemplate = {
            bar: 'bar'
        };

        console.log('creating test objects');

        for (i = 0; i < nbObjects; ++i) {
            objectKeys.push(libuuid.create());
        }

        vasync.forEachParallel({
            func: function writeObject(objectUuid, done) {
                var newObjectValue = jsprim.deepCopy(valueTemplate);
                newObjectValue.uuid = objectUuid;
                morayClient.putObject(TEST_BUCKET_NAME, objectUuid,
                    newObjectValue, done);
            },
            inputs: objectKeys
        }, next);
    },
    function loadDataMigrations(_, next) {
        console.log('loading data migrations');
        morayBuckets.loadDataMigrations({
            migrationsRootPath: dataMigrationsDirPath,
            log: TEST_LOGGER
        }, function onMigrationsLoaded(loadMigrationsErr, migrations) {
            console.log('migrations:', migrations);
            dataMigrations = migrations;
            next(loadMigrationsErr);
        });
    },
    function startBucketsInit(_, next) {
        console.log('start bucket init');

        bucketsInitializer = new morayBuckets.MorayBucketsInitializer({
            bucketsConfig: bucketsConfig,
            dataMigrations: dataMigrations,
            log: TEST_LOGGER,
            /*
             * We do not necessarily define maxBucketsSetupAttempts and
             * maxBucketsReindexAttempts here so that the buckets initializer
             * tries indefinitely, unless it encounters a non-transient error,
             * in which case an 'error' event is emitted and the process aborts
             * (if no error event handler is set) or the error event handler is
             * called.
             *
             */
            maxBucketsSetupAttempts: 10,
            maxBucketsReindexAttempts: 10,
            morayClient: morayClient
        });

        /*
         * We do not necessarily add an 'error' event handler. A program like a
         * Triton component's main program would not add one so that it aborts
         * when a non-transient error (such as an invalid bucket config) occurs
         * during the moray buckets initialization process.
         */
        bucketsInitializer.on('error',
            function onBucketsInitError(bucketsInitErr) {
                morayClient.close();
                console.error('Error:', bucketsInitErr);
            });

        bucketsInitializer.on('done', function onBucketsInitDone() {
            morayClient.close();
            console.log('All buckets init done successfully');
        });

        /*
        bucketsInitializer.on('setup-done', function onBucketsSetupDone() {
            console.log('Buckets setup done!');
        });


        bucketsInitializer.on('reindex-done', function onBucketsSetupDone() {
            console.log('Buckets reindex done!');
        });


        bucketsInitializer.on('data-migrations-done',
            function onBucketsSetupDone() {
                console.log('Data migrations done!');
            });
        */

        bucketsInitializer.start();


        next();
    }
]}, function morayBucketsInitStarted(morayInitStartErr) {
    if (morayInitStartErr) {
        morayClient.close();
        console.error('Moray buckets failed to start:', morayInitStartErr);
    } else {
        console.log('Moray buckets init started successfully');
    }
});
