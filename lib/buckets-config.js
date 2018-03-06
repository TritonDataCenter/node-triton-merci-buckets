/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var assert = require('assert-plus');

function modelToBucketName(modelName, bucketsConfig) {
    assert.string(modelName, 'modelName');
    assert.object(bucketsConfig, 'bucketsConfig');

    if (!bucketsConfig.hasOwnProperty(modelName)) {
        return;
    }

    return bucketsConfig[modelName].name;
}

module.exports = {
    modelToBucketName: modelToBucketName
};