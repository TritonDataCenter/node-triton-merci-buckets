<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2016, Joyent, Inc.
-->

This repository is part of the Joyent Triton project. See the [contribution
guidelines](https://github.com/joyent/triton/blob/master/CONTRIBUTING.md) --
*Triton does not use GitHub PRs* -- and general documentation at the main
[Triton project](https://github.com/joyent/triton) page.

## Introduction

A significant number of Triton projects use [the Moray
database](https://github.com/joyent/moray) in order to store data persistently.

Moray uses "buckets" to store collections of related data. In general, before
being able to store data in Moray, a Moray consumer needs to:

1. initialize the buckets it intents to use
2. perform data migrations on data that is already stored

For various reasons, those two tasks are not necessarily straightforward.

This module aims at providing, via an interface that is easy to use, a robust
implementation of those two tasks that uses the best practices developed over
time by the Triton team.

### Important design and implementation choices

#### Buckets creation/updates, reindexing and data migrations handled as a unit

#### field used to store "data version" not configurable

## Usage

```
var moray = require('moray');
var MorayBucketsInitializer = require('moray-buckets').MorayBucketsInitializer;

var bucketsConfig = {
    fooModel: {
        name: 'foo',
        schema: {
            index: {
                foo: { type: 'string' },
                bar: { type: 'string' },
                baz: { type: 'string' },
                another_indexed_property: { type: 'string' },
                /*
                 * This is required to support data migrations. See "Data
                 * migrations section below"
                 */
                data_version: { type: 'number' }
            },
            options: {
                version: 42
            }
        }
    },
    barModel: {
        name: 'bar',
        schema: {
            index: {
                foo: { type: 'string' },
                bar: { type: 'string' },
                baz: { type: 'string' },
                another_indexed_property: { type: 'string' },
                /*
                 * This is required to support data migrations. See "Data
                 * migrations" section below.
                 */
                data_version: { type: 'number' }
            },
            options: {
                version: 43
            }
        }
    }
};

var morayClient = moray.createClient(morayConfig);

/*
 * The path "dataMigrationsPath" should be a directory with the following
 * structure:
 *
 * fooModel/
 *    001-foo-to-bar.js
 * barModel/
 *    001-foo-to-baz.js
 *    002-baz-to-other-indexed-property.js
 */
morayBucketsInitializer = new MorayBucketsInitializer({
    bucketsConfig: bucketsConfig,
    dataMigrationsPath: dataMigrationsPath,
    log: bunyan.createLogger({name: 'moray-buckets-init'}),
    morayClient: morayClient
});

morayBucketsInitializer.on('error', function bucketsInitError(bucketInitErr) {
    /*
     * Called when a non-transient, non-recoverable error occurs.
    */
    console.error('non-transient error, moray buckets initialization aborted');
});

morayBucketsInitializer.on('buckets-setup-done', function onBucketsSetup() {
    /*
     * Called when all buckets passed as "bucketsConfig" have been created
     * successfully, but not * yet reindexed. Data migrations have also not
     * completed.
     */
    console.log('All buckets are now setup!');
    console.log(morayBucketsInitializer.status());
});

morayBucketsInitializer.on('buckets-reindex-done', function onBucketsReindex() {
    /*
     * Called when all buckets passed as "bucketsConfig" have been created _and_
     * reindexed. At this point, it is safe to perform searches that rely on
     * indexed fields.
     */
    console.log('All buckets are now reindexed!');
});

morayBucketsInitializer.on('data-migrations-done', function onDataMigrated() {
    /*
     * Called when all buckets passed as "bucketsConfig" have been created _and_
     * reindexd, _and_ * after all data migrations have completed successfully.
     */
    console.log('All data migrations have completed successfully!');
});

morayBucketsInitializer.on('done', function onDone() {
    /*
     * This is called when all 'buckets-setup-done', 'buckets-reindex-done' and
     * 'data-migrations-done' events have been emitted.
     */
    console.log('All Moray buckets are initialized!');
});

morayClient.on('connect', function onMorayClientConnected() {
    morayBucketsInitializer.start();
});
```

MorayBucketsInitializer();

## API

### MorayBucketsInitializer constructor

```
var morayBuckets = require('moray-buckets').MorayBucketsInitializer;
var bucketsInitializer = new MorayBucketsInitializer(options);
```

`options` is an object with the following properties:

name | type | required | description
-----|------|----------|------------
bucketsConfig | object | yes | an object describing the configuration of all buckets to initialize
dataMigrationsPath | string | no | the path to a directory that stores data migrations modules. If not set, no data migration is performed as part of the buckets initialization process
log | object | yes | a bunyan logger used by the buckets initializer to log messages
morayClient | object | yes | a moray client object created with the 'moray' npm module

Once a MorayBucketsInitializer instance is created, the buckets initialization
process does not start automatically. In order to start the initialization
process, the `start()` method needs to be called.

### MorayBucketsInitializer.start()

Starts the buckets initialization process. It performs the following steps in
sequence:

1. Creates or updates all moray buckets

2. Reindexes all moray buckets

3. If a `dataMigrationsPath` key is present in the `options` argument passed to
   the constructor, it runs all data migrations

Once the `start()` method is called, the instance on which it was called may
emit events. See the "Events" section below for more details.

If called more than once, `start()` will throw a
`BucketsInitAlreadyStartedError`.

### MorayBucketsInitializer.status()

Returns an object that represents the current state of the buckets
initialization process. The returned object has the following form:

```
{
    bucketsSetup: {
        state: 'DONE',
        /*
         * Only present if the buckets creation/update process encountered an error.
         */
        latestError: errorObject
    },
    bucketsReindex: {
        state: 'DONE',
        /*
         * Only present if the reindexing process encountered an error.
         */
        latestError: errorObject
    },
    dataMigrations: {
        state: 'DONE',
        /*
         * Only present if data migrations are specified in the
         * MorayBucketsInitializer constructor. Each key of the "latestErrors"
         * corresponds to a model name as specified in the "bucketsConfig"
         * parameter passed to the MorayBucketsInitializer constructor.
         */
        latestErrors: {
            model_name: someErrorObject
            other_model_name: anotherErrorObject
        }
    }
}
```

Each `state` value in the status object above can have the following values:

- `NOT_STARTED`
- `STARTED`
- `DONE`
- `ERROR`

### Events

#### 'error'

Emitted when a non-transient error was encountered. A non-transient error can be
the result of:

* a non-transient error at the Moray server/Moray client layer

* the maximum number of retries being reached by any of the buckets
  initialization step described above

#### 'buckets-setup-done'

Emitted when the first step of the buckets initialization process
(creating/updating all buckets) has completed successfully. At this point
read/write operations on all buckets should succeed, but:

* `findObjects` requests may fail, as all objects may not be completely
  reindexed (see the `buckets-reindex-done` event for that use case)

* data migrations haven't run, so some objects may be outdated

#### 'buckets-reindex-done'

Emitted when all objects have been reindexed. Due to the way Moray caches
buckets schemas, it is still not necessarily safe to perform `findObjects`
requests without using the `requireIndexes` options, as they may return
erroneous results until all bucket caches are refreshed across all Moray
instances.

#### 'data-migrations-done'

Emitted when all data migrations have completed successfully.

### Errors

#### `BucketsInitAlreadyStartedError`

#### `InvalidDataMigrationFileNamesError`

#### `InvalidIndexesRemovalError`

#### `SchemaChangesSameVersionError`

## Data migrations

Data migrations are an optional part of the buckets initialization process. If
no `dataMigrationsPath` field is present in the `options` object passed to the
`MorayBucketsInitializer` object, no data migration will be performed.

Data migrations currently use a fixed indexed field to record the "data version"
for each record in a given bucket. That field is named `data_version`.

Data versions for all records start at `1`, and object that do not have a
`data_version` field are considered to not have any version.

### Structure on disk

### Example data migration

Each migration that migrates records for a given model from one version to the
next must be implemented as a node module that implements the following
interface:

* It exports a `DATA_VERSION` field that identifies what the target version for
  that migration module is.

* It exports a `migrateRecord` function `function migrateRecord(record)` where
  `record` is the record to migrate.

The `migrateRecord` function is called synchronously by this module, and it must
return the migrated object. Mutating the `record` object passed as input is
fine.

Here is a simple migration module that handles migrating records from version 0
(i.e records with no version information) to version 1 by setting the field
`bar` of all objects to `foo`:

```
var assert = require('assert-plus');

var DATA_VERSION = 1;

module.exports = {
    DATA_VERSION: DATA_VERSION,
    migrateRecord: function migrateRecord(record) {
        assert.object(record, 'record');
        record.value.bar = 'foo';
        record.value.data_version = DATA_VERSION;
        return record;
    }
};
```

## Development

Describe steps necessary for development here.

    make all


## Test

Describe steps necessary for testing here.

    make test

## License

"moray-buckets" is licensed under the [Mozilla Public License version
2.0](http://mozilla.org/MPL/2.0/). See the file LICENSE.
