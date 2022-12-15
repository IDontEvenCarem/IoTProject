const { default: xs, Stream } = require("xstream")
const _ = require('lodash')

/**
 * @template T
 * @param {Stream<T>} stream 
 * @returns {Stream<T>}
 */
function changes_only (stream) {
    /** @type {import("xstream").Subscription | undefined} */
    let sub = undefined
    let last_val = undefined

    return xs.create({
        start(listener) {
            sub = stream.subscribe({
                next(value) {
                    if (_.isEqual(value, last_val)) return;
                    last_val = value
                    listener.next(value)
                },
                error(err) {
                    listener.error(err)
                },
                complete() {
                    listener.complete()
                }
            })
        },
        stop() {
            sub.unsubscribe()
        }
    })
}

module.exports = {
    changes_only
}