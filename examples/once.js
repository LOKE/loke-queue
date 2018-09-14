"use strict";

var Connection = require("../").Connection;

var connection = new Connection("amqp://localhost");

var queue = connection.queue("blah", { maxConcurrent: 20 });

queue.subscribe("message", function(message) {
  // eslint-disable-next-line no-console
  console.log(message);
  return new Promise(function(resolve) {
    setTimeout(resolve, 500);
  });
});

queue.on("ready", function() {
  connection.publish("message", {
    message: "Hello World"
  });

  queue.stop().then(function() {
    return connection.close();
  });
  return;
});
