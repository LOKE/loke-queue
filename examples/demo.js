"use strict";

var psuedoRandomBytes = require("crypto").pseudoRandomBytes;
var Connection = require("../");

var c = new Connection("amqp://guest:guest@127.0.0.1:5672/", {
  exchange: "jobs",
  autoDelete: true,
  durable: false
});

function green(str) {
  return "\u001b[32m" + str + "\u001b[39m";
}

var worker1 = c.queue("jobs", {
  maxConcurrent: 20,
  durable: false,
  autoDelete: true
});
var worker2 = c.queue("jobs", {
  maxConcurrent: 10,
  durable: false,
  autoDelete: true
});

var active1 = [];
var active2 = [];

worker1.subscribe("job", function(msg) {
  active1.push(msg.body);
  // eslint-disable-next-line no-console
  console.log("#1 -- processing...\n", active1);
  return new Promise(function(resolve) {
    setTimeout(resolve, 1000);
  }).then(function() {
    var index = active1.indexOf(msg.body);
    active1.splice(index, 1);
    // eslint-disable-next-line no-console
    console.log("#1 -- processing...\n", active1);
  });
});

worker2.subscribe("job", function(msg) {
  active2.push(msg.body);
  // eslint-disable-next-line no-console
  console.log(green("#2 -- processing..." + active2.join()));
  return new Promise(function(resolve) {
    setTimeout(resolve, 10000);
  }).then(function() {
    var index = active1.indexOf(msg.body);
    active2.splice(index, 1);
    // eslint-disable-next-line no-console
    console.log(green("#2 -- processing..." + active2.join()));
  });
});

worker1.on("ready", function() {
  worker2.on("ready", function() {
    setInterval(function() {
      c.publish("job", psuedoRandomBytes(3).toString("base64"));
    }, 100);
  });
});
