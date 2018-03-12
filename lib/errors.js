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

function BucketsInitAlreadyStartedError() {
    if (!(this instanceof BucketsInitAlreadyStartedError)) {
        throw new Error('BucketsInitAlreadyStartedError must be used as ' +
            'a constructor');
    }

    Error.call(this);

    this.name = this.constructor.name;
    this.message = 'Moray buckets initialization process was already started ' +
        'and cannot be started more than once using the same ' +
        'MorayBucketsInitializer instance';
}
util.inherits(BucketsInitAlreadyStartedError, Error);
exports.BucketsInitAlreadyStartedError = BucketsInitAlreadyStartedError;

function InvalidDataMigrationFileNamesError(fileNames) {
    if (!(this instanceof InvalidDataMigrationFileNamesError)) {
        throw new Error('InvalidDataMigrationFileNamesError must be used as ' +
            'a constructor');
    }

    Error.call(this);

    assert.arrayOfString(fileNames, 'fileNames');
    this.name = this.constructor.name;
    this.message = 'Invalid data migration file name: ' + fileNames.join(',');
}
util.inherits(InvalidDataMigrationFileNamesError, Error);
exports.InvalidDataMigrationFileNamesError = InvalidDataMigrationFileNamesError;

function InvalidIndexesRemovalError(indexes) {
    Error.call(this);

    assert.arrayOfString(indexes, 'indexes');
    this.name = this.constructor.name;
    this.message = 'Invalid removal of indexes: ' + indexes.join(', ');
}
util.inherits(InvalidIndexesRemovalError, Error);
exports.InvalidIndexesRemovalError = InvalidIndexesRemovalError;

function SchemaChangesSameVersionError(bucketName, oldSchema, newSchema) {
    Error.call(this);

    assert.string(bucketName, 'bucketName');
    assert.object(oldSchema, 'oldSchema');
    assert.object(newSchema, 'newSchema');

    this.name = this.constructor.name;
    this.message = 'Schema changed but version did not. Old schema: ' +
        util.inspect(oldSchema) + ', new schema: ' + util.inspect(newSchema);
}
util.inherits(SchemaChangesSameVersionError, Error);
exports.SchemaChangesSameVersionError = SchemaChangesSameVersionError;
