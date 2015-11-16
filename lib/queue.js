'use strict'

var Promise = require('./promise')
var inherits = require('util').inherits
var EventEmitter = require('events').EventEmitter
var AWS = require('aws-sdk')
var SQS = AWS.SQS

var QueueWorker = require('./queue-worker')

inherits(Queue, EventEmitter)
module.exports = Queue

/**
 * Creates a new queue.
 * @class
 * @param {String} params.url - The SQS Queue URL.
 * @param {String} params.awsKey
 * @param {String} params.awsSecret
 * @param {String} params.awsRegion
 * @param {Promise} [options.PromiseLib} - Promise constructor, defaults to native.
 */
function Queue (url, params, options) {
  params = params || {}
  options = options || {}

  EventEmitter.call(this)

  // AWS.config.update({
  //   accessKeyId: params.awsKey,
  //   secretAccessKey: params.awsSecret
  // })

  this.sqs = new SQS({
    apiVersion: '2012-11-05',
    accessKeyId: params.awsKey,
    secretAccessKey: params.awsSecret,
    region: params.awsRegion
  })
  this.url = url
  this.Promise = options.PromiseLib || Promise
}

/**
 * Manually delete a message from the queue.
 * @param  {Message} message
 * @return {Promise}
 */
Queue.prototype.deleteMessage = function (message) {
  return new this.Promise(function (resolve, reject) {
    message.sqs.deleteMessage({
      QueueUrl: message.url,
      ReceiptHandle: message.receiptHandle
    }, function (err, data) {
      if (err) return reject(err)
      resolve()
    })
  })
}

/**
 * @typedef {Object} Queue.Message
 * @property {Queue} queue - The Queue from which this message came.
 * @property {Object} body - Data.
 */
function Message (queue, sqs, url, sqsMessage) {
  this.queue = queue
  this.sqs = sqs
  this.url = url
  this.body = JSON.parse(sqsMessage.Body)
  this.receiptHandle = sqsMessage.ReceiptHandle
}

/**
 * Send a message onto the queue.
 * @param  {URL} url
 * @param  {Message} message
 * @param  {Object} message.body
 * @return {Promise}
 */
Queue.prototype.push = function (message) {
  var sqs = this.sqs
  var url = this.url

  return new this.Promise(function (resolve, reject) {
    var params = {
      MessageBody: JSON.stringify(message.body),
      QueueUrl: url,
      MessageAttributes: {
        someKey: {
          DataType: 'String',
          StringValue: 'Example'
        }
      }
    }
    sqs.sendMessage(params, function (err, data) {
      if (err) return reject(err)
      resolve()
    })
  })
}

/**
 * Manually take n message(s) from the queue.
 *
 * You will need to call .deleteMessage afterwards.
 *
 * @return {Promise<Queue.Message[]>}
 */
Queue.prototype.shift = function (n) {
  var sqs = this.sqs
  var url = this.url

  var queue = this

  if (n === undefined) n = 1
  return new this.Promise(function (resolve, reject) {
    sqs.receiveMessage({
      QueueUrl: url,
      MaxNumberOfMessages: n,
      VisibilityTimeout: 60,
      WaitTimeSeconds: 2
    }, function (err, data) {
      if (err) return reject(err)
      if (!data.Messages) return resolve([])
      return resolve(
        data.Messages.map(function (message) {
          return new Message(queue, sqs, url, message)
        })
      )
    })
  })
}

/**
 * Start pulling jobs off the queue.
 *
 * When a job comes in, a `message` event will be emitted with a {@link Queue.Message} and acknowledgement callback
 *
 * @param  {Number} [maxConcurrent = 10] - Number of jobs to handle concurrently
 * @return {Queue} this
 */
Queue.prototype.listen = function (maxConcurrent) {
  maxConcurrent = maxConcurrent || 10
  var workers = this.workers = []

  for (var i = 0; i < maxConcurrent; i++) {
    workers[i] = new QueueWorker(this.Promise).start(this)
  }

  return this
}

/**
 * Stop all queue workers.
 * @return {Promise} - promise resolved after all active jobs have finished
 */
Queue.prototype.close = function () {
  return this.Promise.all(this.workers.map(function (worker) {
    return worker.stop()
  }))
}
