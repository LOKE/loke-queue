var defer = require('./defer')
var Promise = require('./promise')

module.exports = QueueWorker

function QueueWorker (PromiseLib) {
  this.Promise = PromiseLib || Promise
}

QueueWorker.prototype.handleMessage = function (message) {
  return new this.Promise(function (resolve, reject) {
    var done = false
    var called = false
    var shouldResolve = false
    function ack (err) {
      if (err) throw err
      if (done) throw new Error('Message has already been deleted.')
      done = true

      defer(
        message.queue.deleteMessage(message)
      )
      if (!called) {
        shouldResolve = true
      } else {
        resolve()
      }
    }

    message.queue.emit('message', message, ack)
    called = true
    if (shouldResolve) {
      resolve()
    }
  })
}

QueueWorker.prototype.getJobs = function (queue) {
  if (this._stop) {
    this._stop()
    this._stop = null
    return
  }

  var self = this
  var Promise = this.Promise

  defer(
    queue.shift(5)
    .then(function (messages) {
      return Promise.all(messages.map(function (message) {
        return self.handleMessage(message)
      }))
    })
    .then(
      function () {
        self.getJobs(queue)
      },
      function (err) {
        queue.emit('error', err)
      }
    )
  )
}

QueueWorker.prototype.start = function (queue, handler) {
  this.getJobs(queue)
  return this
}

QueueWorker.prototype.stop = function () {
  var self = this
  return new this.Promise(function (resolve) {
    self._stop = resolve
  })
}
