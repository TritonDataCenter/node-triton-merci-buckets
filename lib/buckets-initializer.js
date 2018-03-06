/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var assert = require('assert-plus');
var backoff = require('backoff');
var events = require('events');
var util = require('util');
var vasync = require('vasync');
var verror = require('verror');

var dataMigrationsCtrl = require('./data-migrations/controller');
var bucketsSetup = require('./moray/buckets-setup');
var bucketsReindex = require('./moray/buckets-reindex');

var DaraMigrationsController = dataMigrationsCtrl.DataMigrationsController;
var dataMigrationErrorTransient =
    dataMigrationsCtrl.dataMigrationErrorTransient;

/*
 * MorayBucketsInitializer instances drive the process that sets up _and_
 * reindexes the moray buckets that need to be present for VMAPI to function
 * properly. They take an instance of the "Moray" constructor and an object that
 * represents the desired configuration of moray buckets used by VMAPI as input.
 *
 * Once an instance of MorayBucketsInitializer has been created, its "start"
 * method can be called to actually start the process.
 *
 * If the process completes successfully, a 'done' event is emitted by a
 * MorayBucketsInitializer instance. If the process encounters an unrecoverable
 * error, it emits an 'error' event.
 */

/*
 * The constructor for the MorayBucketsInitializer class. It derives from
 * events.EventEmitter.
 *
 * Its parameters are:
 *
 * - "options" (mandatory): an object with properties and values that can be
 *   used to tweak the behavior of the initializer. The following properties are
 *   supported:
 *
 *   * "dataMigrations" (optional): an object describing all data migrations
 *     that need to be performed after setting up and reindexing buckets has
 *     completed. See "loadDataMigrations" to know how to load those data
 *     migrations.
 *
 *   * "maxBucketsSetupAttempts" (optional): the number of attempts to setup
 *     (create and/or update) buckets before an 'error' event is emitted.
 *     Its default value is "undefined" and it causes the process to be retried
 *     indefinitely, unless a non-transient error is encountered.
 *
 *   * "maxBucketsReindexAttempts" (optional): the number of attempts to reindex
 *     buckets before an 'error' event is emitted. Its default value is
 *     "undefined" and it causes the process to be retried indefinitely, unless
 *     a non-transient error is encountered.
 *
 *   * "log" (mandatory): the bunyan logger instance to use.
 */
function MorayBucketsInitializer(options) {
    events.EventEmitter.call(this);

    assert.object(options, 'options');

    assert.object(options.bucketsConfig, 'options.bucketsConfig');
    this._bucketsConfig = options.bucketsConfig;

    assert.object(options.dataMigrations, 'options.dataMigrations');
    this._dataMigrations = options.dataMigrations;

    assert.object(options.log, 'options.log');
    this._log = options.log;

    assert.optionalNumber(options.maxBucketsSetupAttempts,
        'options.maxBucketsSetupAttempts');
    this._maxBucketsSetupAttempts = options.maxBucketsSetupAttempts;

    assert.optionalNumber(options.maxBucketsReindexAttempts,
        'options.maxBucketsReindexAttempts');
    this._maxBucketsReindexAttempts = options.maxBucketsReindexAttempts;

    assert.object(options.morayClient, 'options.morayClient');
    this._morayClient = options.morayClient;

    this._dataMigrationsCtrl = new DaraMigrationsController({
        bucketsConfig: options.bucketsConfig,
        migrations: options.dataMigrations,
        log: options.log,
        morayClient: options.morayClient
    });

    this._lastInitError = null;
    this._status = 'NOT_STARTED';
}
util.inherits(MorayBucketsInitializer, events.EventEmitter);

MorayBucketsInitializer.prototype.status = function status() {
    return this._status;
};

/*
 * Returns an object representing the latest error encountered when setting up
 * VMAPI's moray buckets, null otherwise.
 */
MorayBucketsInitializer.prototype.lastInitError = function lastInitError() {
    return this._lastInitError;
};

/*
 * The "start" method can be used to actually start the process of setting up
 * and reindexing VMAPI's moray buckets.
 *
 * Its parameters are:
 *
 * - "moray": an instance of the Moray constructor used to
 * actually perform operations against the moray key/value store.
 *
 * - "morayBucketsConfig": an object that represents the configuration of the
 * buckets that need to be setup in moray for VMAPI to be able to function
 * properly.
 *
 * When the process completes successfully, the 'done' event is emitted on the
 * MorayBucketsInitializer instance.
 *
 * When the process encounters an error, it emits an 'error' event if the error
 * is considered to be unrecoverable. If the error is considered to be
 * recoverable, it restarts the process until it succeeds, or until the maximum
 * number of retries has been reached.
 *
 * If the maximum number of retries has been reached, the 'error' event is
 * emitted.
 *
 * Transient moray errors are considered to be recoverable and non-transient
 * errors (such as bad bucket configuration errors) are considered to be
 * unrecoverable.
 */
MorayBucketsInitializer.prototype.start = function start() {
    var self = this;

    if (self._status !== 'NOT_STARTED') {
        throw new Error('Moray buckets initialization setup already started');
    }

    self._status = 'STARTED';

    vasync.pipeline({arg: {}, funcs: [
        function setupBuckets(arg, next) {
            self._log.info('Starting setting up buckets');
            self._setupBuckets(function bucketsSetup(bucketsSetupErr) {
                if (!bucketsSetupErr) {
                    self._log.info('Buckets setup successfully');
                    self._status = 'BUCKETS_SETUP_DONE';
                } else {
                    self._log.error({err: bucketsSetupErr},
                        'Error when setting up buckets');
                }

                next(bucketsSetupErr);
            });
        },
        function reindexBuckets(arg, next) {
            self._log.info('Starting reindexing buckets');
            self._reindexBuckets(function bucketsReindexed(bucketsReindexErr) {
                if (!bucketsReindexErr) {
                    self._log.info('Buckets reindexed successfully');
                    self._status = 'BUCKETS_REINDEX_DONE';
                } else {
                    self._log.error({err: bucketsReindexErr},
                        'Error when reindexing buckets');
                }

                next(bucketsReindexErr);
            });
        },
        function migrateData(arg, next) {
            self._log.info('Starting migrating data');
            self._runDataMigrations(function dataMigrationsDone(migrationsErr) {
                if (!migrationsErr) {
                    self._log.info('Data migrations ran successfully!');
                } else {
                    self._log.error({err: migrationsErr},
                        'Error when running data migrations');
                }

                next(migrationsErr);
            })
        }
    ]}, function onBucketsInitialized(bucketsInitErr) {
        if (bucketsInitErr) {
            self._log.error({err: bucketsInitErr},
                'Error when initializing moray buckets');
            self._status = 'FAILED';
            // @ts-ignore
            self.emit('error', bucketsInitErr);
        } else {
            self._log.info('Buckets initialized successfully');
            // @ts-ignore
            self.emit('done');
        }
    });
};

MorayBucketsInitializer.prototype._performBackedOffProcess =
function _performBackedOffProcess(processName, fun, options, callback) {
    assert.string(processName, 'processName');
    assert.func(fun, 'fun');
    assert.object(options, 'options');
    assert.optionalNumber(options.maxAttempts, 'options.maxAttempts');
    assert.func(options.isErrTransientFun, 'options.isErrTransientFun');
    assert.func(callback, 'callback');

    var INITIAL_SETUP_BUCKET_BACKOFF_DELAY_MS = 10;
    var MAX_SETUP_BUCKET_BACKOFF_DELAY_MS = 5000;

    var processBackoff = backoff.exponential({
        initialDelay: INITIAL_SETUP_BUCKET_BACKOFF_DELAY_MS,
        maxDelay: MAX_SETUP_BUCKET_BACKOFF_DELAY_MS
    });
    var self = this;

    if (options.maxAttempts !== undefined) {
        processBackoff.failAfter(options.maxAttempts);
    }

    function onProcessDone(processErr) {
        var errTransient = true;

        if (processErr) {
            self._lastInitError = processErr;

            errTransient = options.isErrTransientFun(processErr);
            if (!errTransient) {
                self._log.error({error: processErr},
                    'Non transient error when performing moray initializer ' +
                        'process ' + processName);

                self._log.debug('stopping moray process backoff');
                processBackoff.reset();

                callback(processErr);
                return;
            } else {
                self._log.warn({error: processErr},
                    'Transient error encountered, backing off');
                processBackoff.backoff();
                return;
            }
        } else {
            self._lastInitError = null;
            self._log.info('Moray process done!');
            processBackoff.reset();
            callback();
            return;
        }
    }

    processBackoff.on('ready', function onSetupBucketsBackoffReady() {
        fun(onProcessDone);
    });

    processBackoff.on('backoff', function onMorayProcessBackoff(number, delay) {
        self._log.warn({
            number: number,
            delay: delay
        }, 'Moray process backed off');
    });

    processBackoff.on('fail', function onProcessFail() {
        callback(new Error('Maximum number of tries reached when ' +
            'performing ' + processName));
    });

    processBackoff.backoff();
};

MorayBucketsInitializer.prototype._setupBuckets =
function _setupBuckets(callback) {
    assert.func(callback, 'callback');

    var self = this;

    self._performBackedOffProcess('buckets setup',
        bucketsSetup.setupBuckets.bind(null, {
            morayClient: self._morayClient,
            bucketsConfig: self._bucketsConfig,
            log: self._log
        }), {
            isErrTransientFun:
                bucketsSetup.isBucketsSetupErrorTransient,
            maxAttempts: self._maxBucketsSetupAttempts
        }, callback);
};

MorayBucketsInitializer.prototype._reindexBuckets =
function _reindexBuckets(callback) {
    assert.func(callback, 'callback');

    var self = this;

    self._performBackedOffProcess('buckets reindex',
        bucketsReindex.reindexBuckets.bind(null, self._bucketsConfig, {
            log: self._log,
            morayClient: self._morayClient
        }), {
            isErrTransientFun: function isReindexErrorTransient(err) {
                /*
                 * Reindexing errors are always transient.
                 */
                return true;
            },
            maxAttempts: self._maxBucketsReindexAttempts
        }, callback);
};

MorayBucketsInitializer.prototype._runDataMigrations =
function _runDataMigrations(callback) {
    var self = this;

    self._performBackedOffProcess('data migrations',
        self._dataMigrationsCtrl.runMigrations.bind(self._dataMigrationsCtrl),
        {
            isErrTransientFun: dataMigrationErrorTransient
        }, callback);
};

module.exports = MorayBucketsInitializer;