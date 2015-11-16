'use strict'

var Promise = require('./promise')
var defer = require('./defer')
var amqplib = require('amqplib')
var inherits = require('util').inherits
var EventEmitter = require('events').EventEmitter
var EXCHANGE = 'loke-queue'

module.exports = Connection

inherits(Connection, EventEmitter)
function Connection (url) {
  EventEmitter.call(this)

  this.amqp = amqplib.connect(url)

  var self = this

  defer(
    this.amqp
    .then(
      function (client) {
        self.emit('connect')
      },
      function (err) {
        self.emit('error', err)
      }
    )
  )
}

Connection.prototype._getPublisher = function () {
  if (this._publishChannel) return this._publishChannel

  var channelPromise = this.amqp
  .then(function (client) {
    return client.createChannel()
  })
  .then(function (ch) {
    return ch.assertExchange(EXCHANGE, 'topic', {
      durable: false,
      autoDelete: false
    })
    .then(function () {
      return ch
    })
  })

  return (this._publishChannel = channelPromise)
}

Connection.prototype._getSharedChannel = function () {
  if (this._sharedChannel) return this._sharedChannel

  var channelPromise = this.amqp
  .then(function (client) {
    return client.createChannel()
  })

  return (this._sharedChannel = channelPromise)
}

Connection.prototype.publish = function (routingKey, body) {
  if (Array.isArray(routingKey)) {
    routingKey = routingKey.join('.')
  }

  var content = new Buffer(JSON.stringify(body))

  var options = {
    correlationId: process.domain && process.domain.cid || null,
    contentType: 'application/json',
    contentEncoding: 'utf-8'
  }

  return this._getPublisher()
  .then(function (ch) {
    return ch.publish(
      EXCHANGE,
      routingKey,
      content,
      options
    )
  })
  .then(function () {
    return
  })
}

Connection.prototype.subscribe = function (options, handler) {
  var channelPromise = this._getSharedChannel()
  var connection = this
  options = options || {}

  var queueName = options.queueName

  var promise = channelPromise
  .then(function (ch) {
    return ch.checkQueue(queueName)
    .then(function () {
      ch.prefetch(options.maxConcurrent || 100)
      return ch.assertQueue(queueName, {
        exclusive: false,
        autoDelete: false,
        durable: true
      })
    })
    .then(function (ok) {
      return ok.queue
    })
    .then(function (queue) {
      return subscribe(connection, ch, queue, queueName, options.routingKey, handler)
    })
  })

  defer(promise)
}

function Message (msg) {
  this.body = JSON.parse(msg.content)
  this.headers = msg.headers || {}
}

function subscribe (connection, channel, queue, queueName, routingKey, handler) {
  function handleMessage (msg) {
    var message = new Message(msg)

    return Promise.resolve()
    .then(function () {
      return handler(message)
    })
    .then(
      function () {
        return channel.ack(msg)
      },
      function (err) {
        channel.reject(msg, true)
        throw err
      }
    )
  }

  return channel.bindQueue(queue, EXCHANGE, routingKey)
  .then(function () {
    var options = {noAck: false}
    return channel.consume(queue, function (msg) {
      defer(
        handleMessage(msg)
        .then(null, function (err) {
          connection.emit('error', err)
        })
      )
    }, options)
  })
}
