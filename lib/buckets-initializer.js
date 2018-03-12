/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var assert = require('assert-plus');
var events = require('events');
var jsprim = require('jsprim');
var util = require('util');
var vasync = require('vasync');

var bucketsSetup = require('./buckets-setup');
var bucketsReindex = require('./buckets-reindex');
var dataMigrations = require('./data-migrations');
var dataMigrationsLoader = require('./data-migrations-loader');
var errors = require('./errors');

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
 *   * "dataMigrationsPath" (optional): path to the directory that stores data
 *      migrations files.
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

    assert.optionalString(options.dataMigrationsPath,
        'options.dataMigrationsPath');
    this._dataMigrationsPath = options.dataMigrationsPath;

    assert.object(options.log, 'options.log');
    this._log = options.log;

    assert.optionalNumber(options.maxBucketsSetupAttempts,
        'options.maxBucketsSetupAttempts');
    this._maxBucketsSetupAttempts = options.maxBucketsSetupAttempts;

    assert.optionalNumber(options.maxBucketsReindexAttempts,
        'options.maxBucketsReindexAttempts');
    this._maxBucketsReindexAttempts = options.maxBucketsReindexAttempts;

    assert.optionalNumber(options.maxDataMigrationsAttempts,
        'options.maxDataMigrationsAttempts');
    this._maxDataMigrationsAttempts = options.maxDataMigrationsAttempts;

    assert.object(options.morayClient, 'options.morayClient');
    this._morayClient = options.morayClient;

    this._started = false;

    this._status = {
        bucketsSetup: {
            state: 'NOT_STARTED'
        },
        bucketsReindex: {
            state: 'NOT_STARTED'
        },
        dataMigrations: {
            state: 'NOT_STARTED'
        }
    };
}
util.inherits(MorayBucketsInitializer, events.EventEmitter);

MorayBucketsInitializer.prototype.status = function status() {
    return jsprim.deepCopy(this._status);
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

    if (self._started) {
        throw new errors.BucketsInitAlreadyStartedError();
    }

    self._started = true;

    var migrations;

    vasync.pipeline({funcs: [
        /*
         * If data migrations are not valid, we want to error early, so we load
         * and validate them early.
         */
        function loadAndValidateDataMigrations(_, next) {
            if (!self._dataMigrationsPath) {
                next();
                return;
            }

            dataMigrationsLoader.loadMigrations({
                log: self._log,
                migrationsDirPath: self._dataMigrationsPath
            }, function migrationsLoaded(loadMigrationsErr, loadedMigrations) {
                if (!loadMigrationsErr) {
                    dataMigrations.validateDataMigrations(self._bucketsConfig,
                        loadedMigrations);
                    migrations = loadedMigrations;
                }

                next(loadMigrationsErr);
            });
        },
        function setupBuckets(_, next) {
            self._log.info('Starting setting up buckets');

            self._status.bucketsSetup =
                bucketsSetup.setupBuckets(self._bucketsConfig, {
                    log: self._log,
                    maxAttempts: self._maxBucketsSetupAttempts,
                    morayClient: self._morayClient
                }, function onBucketsSetup(bucketsSetupErr) {
                    if (!bucketsSetupErr) {
                        self._log.info('Buckets setup successfully');
                        self.emit('buckets-setup-done');
                    } else {
                        self._log.error({err: bucketsSetupErr},
                            'Error when setting up buckets');
                    }

                    next(bucketsSetupErr);
                });
        },
        function reindexBuckets(_, next) {
            self._log.info('Starting reindexing buckets');

            self._status.bucketsReindex =
                bucketsReindex.reindexBuckets(self._bucketsConfig, {
                    log: self._log,
                    maxAttempts: self._maxBucketsReindexAttempts,
                    morayClient: self._morayClient
                }, function onBucketsReindex(bucketsReindexErr) {
                    if (!bucketsReindexErr) {
                        self._log.info('Buckets reindexed successfully');
                        self.emit('buckets-reindex-done');
                    } else {
                        self._log.error({err: bucketsReindexErr},
                            'Error when reindexing buckets');
                    }

                    next(bucketsReindexErr);
                });
        },
        function migrateData(_, next) {
            if (!self._dataMigrationsPath) {
                next();
                return;
            }

            self._log.info('Starting migrating data');

            self._status.dataMigrations =
                dataMigrations.runMigrations(migrations, {
                    bucketsConfig: self._bucketsConfig,
                    log: self._log,
                    maxAttempts: self._maxDataMigrationsAttempts,
                    morayClient: self._morayClient
                }, function onDataMigrations(migrationsErr) {
                if (!migrationsErr) {
                    self._log.info('Data migrations ran successfully!');
                    self.emit('data-migrations-done');
                } else {
                    self._log.error({err: migrationsErr},
                        'Error when running data migrations');
                }

                next(migrationsErr);
            });
        }
    ]}, function onBucketsInitialized(bucketsInitErr) {
        if (bucketsInitErr) {
            self._log.error({err: bucketsInitErr},
                'Error when initializing moray buckets');
            self.emit('error', bucketsInitErr);
        } else {
            self._log.info('Buckets initialized successfully');
            self.emit('done');
        }
    });
};

module.exports = MorayBucketsInitializer;
