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

var TEST_LOGGER = bunyan.createLogger({
    name: 'test-schema-migrations'
});

test('Moray schema migrations with transient error', function (t) {
    var morayBucketsInitializer;
    var morayClient;
    var origMorayClientGetBucket;
    var TEST_BUCKET_NAME =
        'moray_buckets_test_schema_migrations_transient_error';
    var TRANSIENT_ERROR_MSG = 'Mocked transient error';

    var TEST_BUCKET_CONFIG_V0 = {
        name: TEST_BUCKET_NAME,
        schema: {
            index: {
                foo: { type: 'string' }
            }
        }
    };

    var testBucketsConfigV0 = {
        test_model: TEST_BUCKET_CONFIG_V0
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
        function initMorayBucketsWithTransientError(_, next) {
            origMorayClientGetBucket = morayClient.getBucket;

            /*
             * Monkey patch moray client's getBucket method to inject a
             * transient error, so that we can test that the moray buckets
             * initializer behaves correctly in that case.
             */
            morayClient.getBucket =
                function mockedGetBucket(bucketName, callback) {
                    assert.string(bucketName, 'bucketName');
                    assert.func(callback, 'callback');

                    callback(new Error(TRANSIENT_ERROR_MSG));
                };

            morayBucketsInitializer  = new MorayBucketsInitializer({
                bucketsConfig: testBucketsConfigV0,
                log: TEST_LOGGER,
                morayClient: morayClient
            });

            morayBucketsInitializer.start();

            morayBucketsInitializer.once('done', onMorayBucketsInitDone);
            morayBucketsInitializer.once('error', onMorayBucketsInitError);

            function onMorayBucketsInitDone() {
                t.ok(false, 'moray buckets init should not complete when ' +
                    'transient error injected');
                morayBucketsInitializer.removeAllListeners('error');
            }

            function onMorayBucketsInitError(morayBucketsInitError) {
                t.iferror(morayBucketsInitError,
                    'moray buckets init should not error when transient ' +
                        'error injected');
                morayBucketsInitializer.removeAllListeners('done');
            }

            next();
        },
        function checkMorayStatusWithTransientErr(_, next) {
            var checksSoFar = 0;
            var MAX_NB_STATUS_CHECKS = 10;
            var STATUS_CHECKS_DELAY = 1000;

            function checkTransientErr() {
                var bucketsSetupLatestErr;
                var expectedErrString = 'Error: ' + TRANSIENT_ERROR_MSG;
                var morayBucketsInitStatus = morayBucketsInitializer.status();

                ++checksSoFar;

                bucketsSetupLatestErr =
                    morayBucketsInitStatus.bucketsSetup.latestError;

                if (bucketsSetupLatestErr &&
                    bucketsSetupLatestErr.toString() ===  expectedErrString) {
                    t.ok(true, 'did get expected status');
                    next();
                } else {
                    if (checksSoFar < MAX_NB_STATUS_CHECKS) {
                        setTimeout(checkTransientErr, STATUS_CHECKS_DELAY);
                    } else {
                        t.ok(false, 'did not get expected status');
                        next();
                    }
                }
            }

            checkTransientErr();
        },
        function removeTransientError(_, next) {
            /*
             * Now, we're restoring the original function that we had modified
             * to introduce a transient error. As a result, the
             * MorayBucketsInitializer instance should be able to complete the
             * initialization of moray buckets, and the 'done' or 'error' events
             * will be emitted. Thus, we need to clear any listener that were
             * previously added for these events before adding new ones that
             * perform the tests that we want to perform now that the transient
             * error is not injected anymore.
             */
            morayBucketsInitializer.removeAllListeners('error');
            morayBucketsInitializer.removeAllListeners('done');

            morayBucketsInitializer.once('done', onMorayBucketsSetupDone);
            morayBucketsInitializer.once('error', onMorayBucketsSetupFailed);

            morayClient.getBucket = origMorayClientGetBucket;

            function onMorayBucketsSetupDone() {
                morayBucketsInitializer.removeAllListeners('error');
                next();
            }

            function onMorayBucketsSetupFailed(morayBucketsSetupErr) {
                t.ifError(morayBucketsSetupErr,
                    'moray buckets init should succeed once transient error ' +
                        'removed');
                morayBucketsInitializer.removeAllListeners('done');
                next();
            }
        }
    ]}, function onAllTestsDone(testsErr) {
        t.ifError(testsErr, 'tests should not error');

        if (morayClient) {
            morayClient.close();
        }

        t.end();
    });
});


test('Moray schema migrations with non-transient error', function (t) {
    var morayBucketsInitializer;
    var morayClient;
    var TEST_BUCKET_NAME =
        'moray_buckets_test_schema_migrations_non_transient_error';

    var TEST_BUCKET_CONFIG_V0 = {
        name: TEST_BUCKET_NAME,
        schema: {
            index: {
                /*
                 * The typo in "booleaan" is intentional: it is used to trigger
                 * what we consider to be a non-transient error when setting up
                 * moray buckets, and test that the moray buckets setup process
                 * handles this error appropriately, in that case by emitting an
                 * 'error' event.
                 */
                docker: { type: 'booleaan' }
            }
        }
    };

    var testBucketsConfigV0 = {
        test_model: TEST_BUCKET_CONFIG_V0
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
        function initMorayBuckets(_, next) {
            morayBucketsInitializer  = new MorayBucketsInitializer({
                bucketsConfig: testBucketsConfigV0,
                log: TEST_LOGGER,
                morayClient: morayClient
            });

            morayBucketsInitializer.start();

            morayBucketsInitializer.once('done', onMorayBucketsInitDone);
            morayBucketsInitializer.once('error', onMorayBucketsInitError);

            function onMorayBucketsInitDone() {
                t.ok(false, 'moray buckets init should not complete when ' +
                    'non=transient error injected');
                morayBucketsInitializer.removeAllListeners('error');
                next();
            }

            function onMorayBucketsInitError(morayBucketsInitError) {
                t.ok(morayBucketsInitError,
                    'moray buckets init should error when non-transient ' +
                        'error injected');
                morayBucketsInitializer.removeAllListeners('done');
                next();
            }

        },
        function checkMorayStatusWithNonTransientErr(_, next) {
            var bucketsSetupLatestErr;
            var expectedErrCauseWithName = 'InvalidBucketConfigError';
            var morayBucketsInitStatus = morayBucketsInitializer.status();

            bucketsSetupLatestErr =
                morayBucketsInitStatus.bucketsSetup.latestError;

            if (verror.hasCauseWithName(bucketsSetupLatestErr,
                expectedErrCauseWithName)) {
                t.ok(true, 'did get expected status');
            } else {
                t.ok(false, 'did not get expected status');
            }

            next();
        }
    ]}, function onAllTestsDone(testsErr) {
        t.ifError(testsErr, 'tests should not error');

        if (morayClient) {
            morayClient.close();
        }

        t.end();
    });
});
