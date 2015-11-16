/* global describe it */
var Queue = require('../')

describe('SQS', function () {
  it('should send and receive messages', function () {
    this.timeout(10000)
    const queue = new Queue(process.env.SQS_URL, {
      awsKey: process.env.AWS_KEY,
      awsSecret: process.env.AWS_SECRET,
      awsRegion: process.env.AWS_REGION
    })
    var wait = new Promise(function (resolve) {
      var done = false
      queue.on('message', function (message, ack) {
        ack()
        if (done) return
        done = true
        if (message.body !== 'Hello World') {
          throw new Error('Unexpected message: ' + JSON.stringify(message.body))
        }
        resolve(queue.close())
      })

      queue.listen(1)
    })

    return queue.push({body: 'Hello World'})
    .then(function () {
      return wait
    })
  })
})
