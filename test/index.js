/* global describe it */
var Connection = require('../')

describe('SQS', function () {
  it('should send and receive messages', function () {
    this.timeout(10000)
    var c = new Connection('amqp://guest:guest@127.0.0.1:5672/', {})

    var wait = new Promise(function (resolve) {
      var done = false
      c.subscribe('demo.test', {
        queueName: 'analytics',
        maxConcurrent: 2
      }, function (message) {
        if (done) return
        done = true
        resolve(c)
      })
    })

    var promises = []
    for (var i = 0; i < 1; i++) {
      c.publish('demo.test', {index: i, alert: new Date()})
    }

    return Promise.all(promises)
    .then(function () {
      return wait
    })
  })
})
