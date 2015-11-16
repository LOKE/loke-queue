var Connection = require('../').Connection

var connection = new Connection('amqp://localhost')

connection.subscribe('demo.*', {
  queueName: 'analytics',
  maxConcurrent: 20
}, function (message) {
  console.log(message)
  return new Promise(function (resolve) {
    setTimeout(resolve, 500)
  })
})

connection.publish('demo.test', {
  message: 'WHUEIFIJWEF'
})
