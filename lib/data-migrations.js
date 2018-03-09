/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var assert = require('assert-plus');
var util = require('util');
var vasync = require('vasync');
var VError = require('verror');

var mod_bucketsConfig = require('./buckets-config');
var performBackedOffProcess =
    require('./backedoff-process').performBackedOffProcess;

/*
 * Validates that data migrations represented by "dataMigrations" are sound. For
 * instance, it checks that for each model that needs to be migrated, its
 * corresponding moray bucket configuration includes a "data_version" indexed
 * field. It also makes sure that versioning of subsequent data migrations for a
 * given model follows a sequence.
 */
function validateDataMigrations(bucketsConfig, dataMigrations) {
    var bucketConfig;
    var bucketName;
    var expectedDataVersion;
    var idx;
    var migrationsForBucket;

    assert.object(bucketsConfig, 'bucketsConfig');
    assert.object(dataMigrations, 'dataMigrations');

    for (bucketName in dataMigrations) {
        bucketConfig = bucketsConfig[bucketName];

        assert.object(bucketConfig, 'bucketConfig');
        assert.object(bucketConfig.schema.index.data_version,
            'data_version indexed field should be present in bucket config');
        assert.equal(bucketConfig.schema.index.data_version.type, 'number',
            'data_version indexed field should be of type \'number\'');

        migrationsForBucket = dataMigrations[bucketName];
        expectedDataVersion = 1;
        /*
         * Validates that all data migrations that need to be performed are
         * valid. For instance, that their DATA_VERSION numbers are a proper
         * sequence starting at 1, and that they export a function named
         * "migrateRecord".
         */
        for (idx = 0; idx < migrationsForBucket.length; ++idx) {
            assert.equal(migrationsForBucket[idx].DATA_VERSION,
                expectedDataVersion, 'Data version of migration ' + (idx + 1) +
                    ' should be ' + expectedDataVersion);
            assert.func(migrationsForBucket[idx].migrateRecord,
                    'migrationsForBucket[' + idx + '].migrateRecord');
            ++expectedDataVersion;
        }
    }
}

function dataMigrationErrorTransient(error) {
    assert.object(error, 'error');

    var idx;
    var nonTransientErrors = [
        /*
         * For now, we consider a bucket not found to be a non-transient error
         * because it's not clear how that error would resolve itself by
         * retrying the data migrations process.
         */
        'BucketNotFoundError',
        'InvalidIndexTypeError',
        'InvalidQueryError',
        /*
         * We consider NotIndexedError errors to be non-transient because data
         * migrations happen *after any schema migration, including reindexing
         * of all affected buckets* is considered to be complete. As a result,
         * when data migrations start, the indexes that are present will not
         * change, and so retrying on such an error would lead to the same error
         * occurring.
         */
        'NotIndexedError',
        /*
         * Unless a specific data migration handles a UniqueAttributeError
         * itself, we consider that retrying that migration would have the same
         * result, so we treat it as a non-transient error.
         */
        'UniqueAttributeError'
    ];

    for (idx = 0; idx < nonTransientErrors.length; ++idx) {
        if (VError.hasCauseWithName(error, nonTransientErrors[idx])) {
            return false;
        }
    }

    return true;
}

function runMigrations(migrations, options, callback) {
    assert.object(migrations, 'migrations');
    assert.object(options, 'options');
    assert.object(options.bucketsConfig, 'options.bucketsConfig');
    assert.object(options.log, 'options.log');
    assert.optionalNumber(options.maxAttempts, 'options.maxAttempts');
    assert.object(options.morayClient, 'options.morayClient');
    assert.func(callback, 'callback');

    var bucketsConfig = options.bucketsConfig;
    var dataMigrationStatus = {
        completed: {},
        state: 'STARTED'
    };
    var log = options.log;
    var maxAttempts = options.maxAttempts;
    var morayClient = options.morayClient;

    log.info({migrations: migrations}, 'Running data migrations');

    performBackedOffProcess('data migrations',
        _tryRunMigrations.bind(null, migrations, dataMigrationStatus, {
            bucketsConfig: bucketsConfig,
            log: log,
            morayClient: morayClient
        }), {
            isErrTransientFun: dataMigrationErrorTransient,
            log: log,
            maxAttempts: maxAttempts
        }, callback);

    return dataMigrationStatus;
}

function _tryRunMigrations(migrations, status, options, cb) {
    assert.object(migrations, 'migrations');
    assert.object(status, 'status');
    assert.object(options, 'options');
    assert.object(options.bucketsConfig, 'options.bucketsConfig');
    assert.object(options.log, 'options.log');
    assert.object(options.morayClient, 'options.morayClient');
    assert.func(cb, 'cb');

    var bucketsConfig = options.bucketsConfig;
    var log = options.log;
    var modelNames = Object.keys(migrations);
    var morayClient = options.morayClient;

    /*
     * We run data migrations for separate models in *parallel* on purpose. Data
     * migrations are heavily I/O bound, and the number of records for each
     * "model" (or Moray bucket) can vary widely. Thus, performing them in
     * sequence would mean that the migration of a model with very few objects
     * could be significantly delayed by the migration of a model with a much
     * higher number of objects. Instead, data migrations process objects in
     * chunks of a bounded number of objects (currently 1000, the default Moray
     * "page" limit), and thus these data migrations are interleaved, making
     * none of them blocked on each other.
     */
    vasync.forEachParallel({
        func: function runAllMigrationsForSingleModel(modelName, done) {
            _runMigrationsForModel(modelName, migrations[modelName], status, {
                bucketsConfig: bucketsConfig,
                log: log,
                morayClient: morayClient
            }, done);
        },
        inputs: modelNames
    }, function allMigrationsDone(migrationsErr) {
        if (!migrationsErr) {
            status.state = 'DONE';
        }

        cb(migrationsErr);
    });

}

function _runMigrationsForModel(modelName, dataMigrations, status, options,
    callback) {
    assert.string(modelName, 'modelName');
    assert.arrayOfObject(dataMigrations, 'dataMigrations');
    assert.object(options, 'options');
    assert.object(options.bucketsConfig, 'options.bucketsConfig');
    assert.object(options.log, 'options.log');
    assert.object(options.morayClient, 'options.morayClient');
    assert.func(callback, 'callback');

    var bucketsConfig = options.bucketsConfig;
    var log = options.log;
    var morayClient = options.morayClient;

    log.info('Starting data migrations for model %s', modelName);

    vasync.forEachPipeline({
        func: function runSingleMigration(migration, next) {
            assert.number(migration.DATA_VERSION, 'migration.DATA_VERSION');
            assert.ok(migration.DATA_VERSION >= 1,
                'migration.DATA_VERSION >= 1');

            _runSingleMigration(modelName, migration, {
                bucketsConfig: bucketsConfig,
                log: log,
                morayClient: morayClient
            }, function onMigration(migrationErr) {
                if (migrationErr) {
                    if (!status.latestErrors) {
                        status.latestErrors = {};
                    }

                    status.latestErrors[modelName] = migrationErr;
                    status.state = 'ERROR';

                    log.error({err: migrationErr},
                        'Error when running migration to data version: ' +
                            migration.DATA_VERSION);
                } else {
                    if (status.latestErrors && status.latestErrors[modelName]) {
                        delete status.latestErrors[modelName];
                    }

                    if (status.latestErrors &&
                        Object.keys(status.latestErrors).length === 0) {
                        delete status.latestErrors;
                    }

                    status.completed[modelName] = migration.DATA_VERSION;

                    log.info('Data migration to data version: ' +
                        migration.DATA_VERSION + ' ran successfully');
                }

                next(migrationErr);
            });
        },
        inputs: dataMigrations
    }, function onAllMigrationsDone(migrationsErr, results) {
        var err;

        if (migrationsErr) {
            err = new VError(migrationsErr, 'Failed to run data migrations');
        }

        callback(err);
    });
}

function _runSingleMigration(modelName, migration, options, callback) {
    assert.string(modelName, 'modelName');
    assert.object(migration, 'migration');
    assert.func(migration.migrateRecord, 'migration.migrateRecord');
    assert.number(migration.DATA_VERSION, 'migration.DATA_VERSION');
    assert.ok(migration.DATA_VERSION >= 1,
            'migration.DATA_VERSION >= 1');
    assert.object(options, 'options');
    assert.object(options.bucketsConfig, 'options.bucketsConfig');
    assert.object(options.log, 'options.log');
    assert.object(options.morayClient, 'options.morayClient');
    assert.func(callback, 'callback');

    var bucketsConfig = options.bucketsConfig;
    var context = {};
    var log = options.log;
    var morayClient = options.morayClient;
    var version = migration.DATA_VERSION;

    log.info('Running migration for model %s to data version: %s', modelName,
        version);

    function processNextChunk() {
        vasync.pipeline({arg: context, funcs: [
            function findRecords(ctx, next) {
                _findRecordsToMigrate(modelName, version, {
                    bucketsConfig: options.bucketsConfig,
                    log: log,
                    morayClient: morayClient
                }, function onFindRecords(findErr, records) {
                    if (findErr) {
                        log.error({err: findErr},
                            'Error when finding records not at version: ' +
                                version);
                    } else {
                        log.info('Found ' + records.length + ' records');
                        ctx.records = records;
                    }

                    next(findErr);
                });
            },
            function migrateRecords(ctx, next) {
                var bucketName = mod_bucketsConfig.modelToBucketName(modelName,
                    bucketsConfig);
                assert.string(bucketName, 'bucketName');

                var migrateRecordFunc = migration.migrateRecord;
                var migratedRecords;
                var records = ctx.records;

                assert.arrayOfObject(records, 'records');

                if (records.length === 0) {
                    next();
                    return;
                }

                migratedRecords = records.map(function migrate(record) {
                    return migrateRecordFunc(record, {log: log});
                });

                log.trace({migratedRecords: migratedRecords},
                    'Migrated records');

                _putBatch(bucketName, migratedRecords, {
                    morayClient: morayClient
                }, next);
            }
        ]}, function onChunkProcessed(chunkProcessingErr) {
            var records = context.records;

            if (chunkProcessingErr) {
                log.error({err: chunkProcessingErr},
                    'Error when processing chunk');
                callback(chunkProcessingErr);
                return;
            }

            if (!records || records.length === 0) {
                log.info('No more records at version: ' + version +
                    ', migration done');
                callback();
            } else {
                log.info('Processed ' + records.length + ' records, ' +
                    'scheduling processing of next chunk');
                setImmediate(processNextChunk);
            }
        });
    }

    processNextChunk();
}

function _findRecordsToMigrate(modelName, version, options, callback) {
    assert.string(modelName, 'modelName');
    assert.number(version, 'version');
    assert.ok(version >= 1, 'version >= 1');
    assert.object(options, 'options');
    assert.object(options.bucketsConfig, 'options.bucketsConfig');
    assert.object(options.log, 'options.log');
    assert.object(options.morayClient, 'options.morayClient');
    assert.func(callback, 'callback');

    var bucketName = mod_bucketsConfig.modelToBucketName(modelName,
        options.bucketsConfig);
    var log = options.log;
    var morayClient = options.morayClient;
    var morayFilter;
    var records = [];
    var RETRY_DELAY_IN_MS = 10000;

    /*
     * !!!! WARNING !!!!
     *
     * When updating these LDAP filters, make sure that they don't break the
     * assumption below that an InvalidQueryError can be treated as a transient
     * error (See below why.).
     *
     * !!!! WARNING !!!!
     */
    if (version === 1) {
        /*
         * Version 1 is special, in the sense that there's no anterior version
         * for which data_version has a value. Instead, the version before
         * version 1 is represented by an absence of value for the data_version
         * field.
         */
        morayFilter = '(!(data_version=*))';
    } else {
        /*
         * For any migration whose version number is greater than one, they only
         * migrate records at version N - 1. This is safe because:
         *
         * 1. all new records created are created at the latest version
         *    supported by VMAPI
         *
         * 2. migrations are always done in sequence, starting from the
         *    migration that migrates records without a data_version to records
         *    with a data_version === 1.
         */
        morayFilter = util.format('(|(!(data_version=*))(data_version=%s))',
            version - 1);
    }

    log.debug({filter: morayFilter, version: version},
        'generated LDAP filter to find records at version less than given ' +
            'version');

    /*
     * It would be useful to pass either the requireIndexes: true or
     * requireOnlineReindexing: true options to findObjects here, as that would
     * allow us to make sure that we can actually rely on the results from this
     * query. However:
     *
     * 1. We don't want to rely on a specific version of the Moray server.
     *    Support for these options is fairly new (see
     *    http://smartos.org/bugview/MORAY-104 and
     *    http://smartos.org/bugview/MORAY-428) and being able to perform data
     *    migrations is a basic requirement of the service, so we don't want to
     *    prevent that from happening if Moray was rolled back in a DC to a
     *    version that doesn't support those flags. Moreover, at the time data
     *    migrations were added, the latest version of the manta-moray image in
     *    the "support" channel of updates.joyent.com did not include MORAY-104
     *    or MORAY-428.
     *
     * 2. Since this filter uses only one field, Moray already has a mechanism
     *    that will return an InvalidQueryError in case this field is not
     *    indexed, which effectively acts similarly to those two different
     *    options mentioned above.
     */
    var req = morayClient.findObjects(bucketName, morayFilter);

    req.once('error', function onRecordsNotAtVersionError(err) {
        log.error({err: err},
            'Error when finding next chunk of records to migrate');

        if (VError.hasCauseWithName(err, 'InvalidQueryError')) {
            /*
             * We treat InvalidQueryError here as a transient error and retry
             * when it occurs because:
             *
             * 1. We know that the LDAP filter passed to the findObjects request
             *    uses only one field (data_version), and that field is present
             *    as an indexed field in the buckets config (see the
             *    validateDataMigrations function in this file).
             *
             * 2. We know that data migrations are run *after* reindexing of all
             *    buckets is completed and successful.
             *
             * As a result, we can rely on this field being indexed and
             * searchable, and we know that an InvalidQueryError is returned by
             * the Moray server only when the bucket cache of the Moray instance
             * that responded has not been refreshed yet.
             */
            log.info('Scheduling retry in ' + RETRY_DELAY_IN_MS + ' ms');
            setTimeout(function retry() {
                log.info({version: version},
                        'Retrying to find records at version less than');
                _findRecordsToMigrate(modelName, version, options, callback);
            }, RETRY_DELAY_IN_MS);
        } else {
            callback(err);
        }
    });

    req.on('record', function onRecord(record) {
        records.push(record);
    });

    req.once('end', function onEnd() {
        callback(null, records);
    });
}

/*
 * Generates a Moray batch request to PUT all objects in the array of objects
 * "records", and call "callback" when it's done.
 *
 * @params {String} modelName: the name of the model (e.g "vms", "vm_role_tags",
 *   "server_vms") for which to generate a PUT batch operation
 *
 * @params {ArrayOfObjects} records
 *
 * @params {Function} callback(err)
 */
function _putBatch(bucketName, records, options, callback) {
    assert.string(bucketName, 'bucketName');
    assert.arrayOfObject(records, 'records');
    assert.object(options, 'options');
    assert.object(options.morayClient, 'options.morayClient');
    assert.func(callback, 'callback');

    var morayClient = options.morayClient;

    morayClient.batch(records.map(function generateVmPutBatch(record) {
        return {
            bucket: bucketName,
            operation: 'put',
            key: record.value.uuid,
            value: record.value,
            etag: record._etag
        };
    }), function onBatch(batchErr/* , meta */) {
        /*
         * We don't care about the data in "meta" for now (the list of etags
         * resulting from writing all records), and adding it later would be
         * backward compatible.
         */
        callback(batchErr);
    });
}

module.exports = {
    runMigrations: runMigrations,
    validateDataMigrations: validateDataMigrations
};
