"use strict";

const amqplib = require("amqplib");
const ulid = require("ulid");
const inherits = require("util").inherits;
const EventEmitter = require("events").EventEmitter;

const Promise = require("./promise");
const defer = require("./defer");
const lokeQueueVersion = require("../package").version;

module.exports = Connection;

inherits(Connection, EventEmitter);
/**
 * Create a connection to a RabbitMQ server.
 *
 * Once connected, it will emit a `connect` event.
 *
 * If the connection process fails, an `error` event will be emitted.
 *
 * @class
 * @param {String} url - The URL of the RabbitMQ Server. E.g. `amqp://guest:guest@127.0.0.1:5672/`
 * @param {Object} options
 * @param {String} [options.exchange = 'loke-queue'] - Name of the exchange for scoping event names.
 * @param {Boolean} [options.durable = false] - If true, the exchange will be durable.
 * @param {Boolean} [options.autoDelete = false] - If true, the exchange will autodelete.
 * @param {Object} [options.package]
 * @param {String} [options.package.name]
 * @param {String} [options.package.version]
 */
function Connection(url, options) {
  EventEmitter.call(this);

  options = options || {};

  var pkg = options.package || {};

  var connectionOptions = {
    clientProperties: {
      applicationName: pkg.productName || process.title || "Unknown",
      product: pkg.name || "?",
      version: pkg.version || "?",
      information:
        "https://loke.github.io/loke-queue/doc/loke-queue/" + lokeQueueVersion
    }
  };

  var connecting = (this.amqp = amqplib.connect(
    url,
    connectionOptions
  ));

  var exchange = (this.exchange = options.exchange || "loke-queue");

  var self = this;

  var isDurable = options.durable === undefined ? true : options.durable;
  this.durable = isDurable;
  this.autoDelete = options.autoDelete || false;

  var exchangePromise = connecting
    .then(function(client) {
      return client.createChannel().then(function(ch) {
        return ch
          .assertExchange(exchange, "topic", {
            durable: isDurable,
            autoDelete: options.autoDelete || false
          })
          .then(function() {
            self.emit("connect");
            return ch;
          });
      });
    })
    .then(null, function(err) {
      self.emit("error", err);
    });

  this._exchangePromise = exchangePromise;

  this._publishChannel = exchangePromise;

  defer(exchangePromise);
}

Connection.prototype._getWaitExchangeName = function(retryAfterMilliseconds) {
  retryAfterMilliseconds = Math.round(Number(retryAfterMilliseconds));
  var waitExchange = this.exchange + "-wait-" + retryAfterMilliseconds;
  var waitQueueName = waitExchange + "-all";
  var isDurable = this.durable;
  var autoDelete = this.autoDelete || false;
  var exchange = this.exchange;

  return this._exchangePromise.then(function(ch) {
    return ch
      .assertExchange(waitExchange, "topic", {
        durable: isDurable,
        autoDelete: autoDelete
      })
      .then(function() {
        return ch.assertQueue(waitQueueName, {
          exclusive: false,
          autoDelete: autoDelete,
          durable: isDurable,
          deadLetterExchange: exchange,
          messageTtl: retryAfterMilliseconds
        });
      })
      .then(function(ok) {
        return ch.bindQueue(ok.queue, waitExchange, "#").then(function() {
          return waitExchange;
        });
      });
  });
};

/**
 * Close the connection
 * @return {Promise}
 */
Connection.prototype.close = function() {
  return this._exchangePromise
    .then(ch => ch.close())
    .then(() => this.amqp.close());
};

/**
 * Publish an event to the topic exchange.
 *
 * The routing key is a dot separated name of the event.
 * Events can be subscribed to using wildcards at any part of the routing key.
 *
 *
 * @param  {String | String[]} routingKey - The event name. E.g. `user.create`. Also supports using ['user', 'create'] which is sometimes more convenient.
 * @param  {*} body - The data to send. This contains arbitrary JSON-serializable message data.
 * @param  {Object} [opts]
 * @param  {Object<String, String>} [opts.headers] - Metadata to send along with the message.
 * @return {Promise} - a promise resolved after send confirmation
 */
Connection.prototype.publish = function(routingKey, body, opts = {}) {
  if (Array.isArray(routingKey)) {
    routingKey = routingKey.join(".");
  }

  var content = new Buffer(JSON.stringify(body));
  var exchange = this.exchange;

  var options = {
    correlationId: opts.cid || ulid(),
    contentType: "application/json",
    contentEncoding: "utf-8",
    headers: opts.headers || {}
  };

  return this._publishChannel
    .then(ch => ch.publish(exchange, routingKey, content, options))
    .then(() => undefined);
};

/**
 * Subscribe to an event using a temporary queue.
 *
 * You must call `.stop()` on the resultant queue.
 *
 * @param  {String | String[]} routingKey - The event names to receive.
 * @param  {Function} handler - The function to be called with the {@link Connection.Message}
 * @return {Queue}
 */
Connection.prototype.subscribe = function(routingKey, handler) {
  return this.queue({
    durable: false,
    autoDelete: true
  }).subscribe(routingKey, handler);
};

/**
 * Receive messages from a queue.
 *
 * You must call `.stop()` on the resultant queue.
 *
 * @param  {String} [queueName] - The name of the queue to receive events through. If not provided, then a new queue is created. If you want to share a queue between processes, simply use the same string here.
 * @param  {Object} [options]
 * @param  {?Number} [options.retryAfterMilliseconds = false] - If set, rejected messages will be re-sent to the queue after `options.retry` milliseconds.
 * @param  {Boolean} [options.durable = true] - If true, the queue will survive RabbiqMQ broker restarts.
 * @param  {Boolean} [options.exclusive = false] - If true, scopes the queue to the connection.
 * @param  {Boolean} [options.autoDelete = false] - Automatically delete the queue when there are no consumers.
 * @param  {Number} [options.maxConcurrent = 100] - Maximum number of messages for this consumer to receive from the queue which can be waiting acknowledgement. This is the maximum concurrency of `handler(message)` calls.
 * @return {Queue}
 */
Connection.prototype.queue = function(queueName, options) {
  if (typeof queueName === "object") {
    if (options) throw new Error("Invalid queue name: " + queueName);
    options = queueName;
    queueName = undefined;
  }
  options = options || {};
  var defaultExchange = this.exchange;
  var exchangePromise = this._exchangePromise;
  var consumeOptions = { noAck: false };
  var self = this;

  var promise = this.amqp
    .then(function(client) {
      return client.createChannel();
    })
    .then(function(ch) {
      function handleMsg(msg) {
        if (msg === null) {
          // the consumer was cancelled by RabbitMQ
          return;
        }
        var message = new Message(msg);

        function ack(err) {
          if (err) {
            ch.reject(msg, false);
            self.emit("error", err);
            return;
          }
          ch.ack(msg);
        }

        defer(queue.handleMessage(message, ack));
      }

      ch.prefetch(options.maxConcurrent);
      return Promise.resolve()
        .then(function() {
          if (!options.retryAfterMilliseconds) return undefined;
          return self._getWaitExchangeName(options.retryAfterMilliseconds);
        })
        .then(function(deadLetterExchange) {
          return ch.assertQueue(queueName, {
            exclusive: options.exclusive || false,
            autoDelete: options.autoDelete || false,
            durable: options.durable === undefined ? true : !!options.durable,
            deadLetterExchange: deadLetterExchange
          });
        })
        .then(function(ok) {
          return ch
            .consume(ok.queue, handleMsg, consumeOptions)
            .then(function(okConsume) {
              return {
                queue: ok.queue,
                channel: ch,
                consumerTag: okConsume.consumerTag
              };
            });
        });
    });

  var bindPromises = [];

  function emitReadyWhenReady() {
    defer(
      Promise.all(bindPromises).then(function() {
        queue.emit("ready");
        bindPromises = [];
      })
    );
  }

  var bind = function(routingKey) {
    if (!bindPromises.length) {
      setTimeout(emitReadyWhenReady, 0);
    }
    var bindPromise = promise
      .then(function(details) {
        return exchangePromise.then(function() {
          return details.channel.bindQueue(
            details.queue,
            defaultExchange,
            routingKey
          );
        });
      })
      .then(null, function(err) {
        self.emit("error", err);
      });

    bindPromises.push(bindPromise);

    defer(bindPromise);
  };

  var cancel = function() {
    // Instructs the server to stop sending messages
    return promise.then(function(details) {
      return details.channel.cancel(details.consumerTag).then(function() {
        return details.channel;
      });
    });
  };

  var queue = new Queue(bind, cancel);

  defer(
    promise.then(null, function(err) {
      self.emit("error", err);
    })
  );

  return queue;
};

inherits(Queue, EventEmitter);

/**
 * Queues are created using {@link Connection#queue}
 * @class Queue
 */
function Queue(bind, cancel) {
  EventEmitter.call(this);
  this.handlers = [];
  this._bind = bind;
  this._cancel = cancel;
  this._busy = 0;
}

Queue.prototype.handleMessage = function(message, ack) {
  var routingPath = message.routingKey.split(".");
  var handlers = this.handlers.filter(function(entry) {
    return entry.path.every(function(name, i) {
      if (name === "*") return true;
      return name === routingPath[i];
    });
  });

  if (!handlers.length) {
    handlers.push({
      handler: function() {
        throw new Error("No handler set up for " + message.routingKey + ".");
      }
    });
  }

  var self = this;

  self._busy++;

  function decBusyCount() {
    self._busy--;
    if (!self._busy) {
      self.emit("drain");
    }
  }

  return Promise.all(
    handlers.map(function(entry) {
      return Promise.resolve().then(function() {
        return entry.handler(message);
      });
    })
  ).then(
    function() {
      ack();
      decBusyCount();
    },
    function(err) {
      decBusyCount();
      ack(err);
    }
  );
};

/**
 * Bind a routing key event name to the queue.
 *
 * For every {@link Connection.Message} which matches the routing key, it will call `handler(message)`. Each message expects an acknowledgement
 * which is done by returning a value or a promise. If you throw an error or return a rejecting promise,
 * the message will be sent back into the queue.
 *
 * This method does not return a promise because it is asynchronous. Instead, it emits an 'error' event upon failure.
 *
 * Once the queue is successfully bound, the queue will emit a `ready` event.
 *
 * @param  {String | String[]} routingKey - The event names to receive.
 * @param  {Function} handler - The consumer function called with every {@link Connection.Message}.
 * @return {Queue} this
 */
Queue.prototype.subscribe = function(routingKey, handler) {
  this._bind(routingKey);

  var routingPath = Array.isArray(routingKey)
    ? routingKey
    : routingKey.split(".");

  this.handlers.push({ path: routingPath, handler: handler });

  return this;
};

/**
 * Stop receiving any new messages from the queue, and wait for current messages to be handled.
 *
 * After calling this method, some messages may still be handled. This is due to the time delay in cancelling the consumer.
 *
 * This closes the internal channel.
 *
 * @return {Promise}
 */
Queue.prototype.stop = function() {
  var self = this;
  return this._cancel().then(function(ch) {
    // wait for active messages before we close the channel:
    if (self._busy) {
      // We need to keep the channel open so we can send the final acknowledgements
      return new Promise(function(resolve) {
        // Wait for it to become unbusy:
        self.once("drain", function() {
          // It's safe to close now:
          resolve(ch.close());
        });
      });
    }

    return ch.close();
  });
};

/**
 * @typedef {Object} Connection.Message
 * @property {*} body
 * @property {Object<String, String>} headers
 * @property {String} routingKey
 * @property {String} exchange
 */
function Message(msg) {
  this.routingKey = msg.fields.routingKey;
  this.exchange = msg.fields.exchange;
  this.body = JSON.parse(msg.content);
  this.headers = msg.properties.headers || {};
}
