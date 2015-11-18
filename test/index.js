/* global describe it */
var Connection = require('../')

describe('SQS', function () {
  it('should send and receive messages', function () {
    this.timeout(10000)
    var c = new Connection('amqp://guest:guest@127.0.0.1:5672/', {autoDelete: true, durable: false})

    var queue = c.queue('analytics', {maxConcurrent: 2, durable: true, autoDelete: true})
    var hasAcked = false
    var wait = new Promise(function (resolve) {
      var done = false
      queue.subscribe('demo.*', function (message) {
        if (done) return
        done = true
        resolve()
        return new Promise(function (resolve) {
          setTimeout(function () {
            resolve()
            hasAcked = true
          }, 100)
        })
      })
    })

    return new Promise(function (resolve) {
      queue.on('ready', function () {
        var promises = []
        for (var i = 0; i < 1; i++) {
          c.publish('demo.test', {index: i, alert: new Date()})
        }
        resolve(
          Promise.all(promises)
          .then(function () {
            return wait
          })
          .then(function () {
            if (hasAcked) throw new Error('Timing error')
            return queue.stop()
            .then(function () {
              if (!hasAcked) throw new Error('The request was still in progress')
            })
          })
        )
      })
    })
  })
})
