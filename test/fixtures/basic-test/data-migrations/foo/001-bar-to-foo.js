/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var assert = require('assert-plus');

var DATA_VERSION = 1;

function migrateRecord(record, options) {
    var log;
    var parsedInternalMetadata;
    var recordValue;

    assert.object(record, 'record');
    assert.object(record.value, 'record.value');
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');

    log = options.log;
    recordValue = record.value;

    if (recordValue.data_version !== undefined) {
        return;
    }

    recordValue.bar = 'foo';
    recordValue.data_version = DATA_VERSION;

    return record;
}

module.exports = {
    migrateRecord: migrateRecord,
    DATA_VERSION: DATA_VERSION
};