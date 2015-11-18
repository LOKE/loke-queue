'use strict'

var Connection = require('../').Connection

var connection = new Connection('amqp://localhost')

var queue = connection.queue('jobs', {maxConcurrent: 20})

queue.subscribe('demo.*', function (message) {
  console.log(message)
  return new Promise(function (resolve) {
    setTimeout(resolve, 500)
  })
})

setTimeout(function () {
  queue.stop()
}, 100)

setTimeout(function () {
  connection.publish('demo.test', {
    message: 'Hello World'
  });
})
