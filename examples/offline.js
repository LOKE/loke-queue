'use strict'

var QUEUE = 'loke-queue-offline'
var EXCHANGE = 'loke-queue-test'

var Connection = require('../').Connection

var connection = new Connection('amqp://localhost', {exchange: EXCHANGE})

var queue = connection.queue(QUEUE, {maxConcurrent: 20})

queue.subscribe('demo.*', function (message) {})

queue.on('ready', function () {
  queue.stop()
  .then(function () {
    // publish some events:
    connection.publish('demo.test', {
      message: 'Sent while offline'
    })
    .then(function () {
      // now pull from the queue and hope we didn't miss anything:
      var queue2 = connection.queue(QUEUE, {maxConcurrent: 20})
      queue2.subscribe('demo.*', function (msg) {
        console.log('While offline: ' + msg.body.message)
      })
    })
  })
})
