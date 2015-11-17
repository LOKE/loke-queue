# loke-queue

[![NPM Version](https://img.shields.io/npm/v/loke-queue.svg)](https://www.npmjs.com/package/loke-queue)
[![Build Status](https://img.shields.io/travis/LOKE/loke-queue/master.svg)](https://travis-ci.org/LOKE/loke-queue)

Simple RabbitMQ wrapper exposing queue functionality.

## Example

```js
var Connection = require('loke-queue').Connection

var connection = new Connection('amqp://localhost')

connection.subscribe('demo.*', {
  queueName: 'jobs',
  maxConcurrent: 20
}, function (message) {
  console.log(message)
  return new Promise(function (resolve) {
    setTimeout(resolve, 500)
  })
})

connection.publish('demo.test', {
  message: 'Hello World'
})
```
