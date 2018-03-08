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

function performBackedOffProcess(processName, fun, options, callback) {
    assert.string(processName, 'processName');
    assert.func(fun, 'fun');
    assert.object(options, 'options');
    assert.func(options.isErrTransientFun, 'options.isErrTransientFun');
    assert.object(options.log, 'options.log');
    assert.optionalNumber(options.maxAttempts, 'options.maxAttempts');
    assert.func(callback, 'callback');

    var INITIAL_SETUP_BUCKET_BACKOFF_DELAY_MS = 10;
    var log = options.log;
    var MAX_SETUP_BUCKET_BACKOFF_DELAY_MS = 5000;
    var processBackoff = backoff.exponential({
        initialDelay: INITIAL_SETUP_BUCKET_BACKOFF_DELAY_MS,
        maxDelay: MAX_SETUP_BUCKET_BACKOFF_DELAY_MS
    });

    if (options.maxAttempts !== undefined) {
        processBackoff.failAfter(options.maxAttempts);
    }

    function onProcessDone(processErr) {
        var errTransient = true;

        if (processErr) {
            errTransient = options.isErrTransientFun(processErr);
            if (!errTransient) {
                log.error({error: processErr},
                    'Non transient error when performing moray initializer ' +
                        'process ' + processName);

                log.debug('stopping moray process backoff');
                processBackoff.reset();

                callback(processErr);
                return;
            } else {
                log.warn({err: processErr.toString()},
                    'Transient error encountered, backing off');
                processBackoff.backoff();
                return;
            }
        } else {
            log.info('Moray process done!');
            processBackoff.reset();
            callback();
            return;
        }
    }

    processBackoff.on('ready', function onSetupBucketsBackoffReady() {
        fun(onProcessDone);
    });

    processBackoff.on('backoff', function onMorayProcessBackoff(number, delay) {
        log.warn({
            number: number,
            delay: delay
        }, 'Moray process backed off');
    });

    processBackoff.on('fail', function onProcessFail() {
        callback(new Error('Maximum number of tries reached when ' +
            'performing ' + processName));
    });

    processBackoff.backoff();
}

module.exports = {
    performBackedOffProcess: performBackedOffProcess
};
