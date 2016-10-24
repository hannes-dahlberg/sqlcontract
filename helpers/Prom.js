var Promise = require('bluebird');
var _ = require('lodash');

module.exports = {
    /**
     * Recursively calls all *promises* in sequence
     * and resolve when all promises are finished.
     *
     * Takes both pure promises and functions returning promises.
     *
     * NOTE: If the array contains functions, these mustn't require parameters,
     * as *sequence* won't pass in any at the moment.
     *
     * @param {Array} promises Array of promises to perform
     * @param {Array} output The output array, do not set!
     * @return {Promise} -> {Array}
     */
    sequence: function sequence(promises, output) {
        // Make sure output is defined
        if (_.isUndefined(output)) { output = []; }

        // Make sure promises are defined
        if (_.isUndefined(promises)) { promises = []; }

        // When finished
        if (promises.length === output.length || output.some(_.isError)) {
            return new Promise(function (resolve, reject) {
                resolve(output);
            });
        }

        // Allow both promises and functions returning promises be used.
        var _promise = _.isFunction(promises[output.length])
            ? promises[output.length]()
            : promises[output.length];

        // Call the promise and then return recursively
        return _promise.then(function (result) {
            // Recursion
            return sequence(promises, output.concat([result]));
        }).catch(function(error) {
            // Recursion
            return sequence(promises, output.concat([error]));
        });
    }
};