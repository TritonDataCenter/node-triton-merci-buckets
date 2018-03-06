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

function InvalidIndexesRemovalError(indexes) {
    Error.call(this);

    assert.arrayOfString(indexes, 'indexes');
    this.name = this.constructor.name;
    this.message = 'Invalid removal of indexes: ' + indexes.join(', ');
}
util.inherits(InvalidIndexesRemovalError, Error);
exports.InvalidIndexesRemovalError = InvalidIndexesRemovalError;

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
