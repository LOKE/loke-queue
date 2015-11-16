'use strict'

module.exports = deferPromise

function deferPromise (promise) {
  promise
  .then(
    null,
    function (err) {
      process.nextTick(function () {
        throw err
      })
    }
  )
}
