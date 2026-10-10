import amqp from 'amqplib';

export const RABBIT_URL = process.env.RABBITMQ_URL || 'amqp://localhost:5672';
export const MAIN_EXCHANGE = 'store.events';
export const RETRY_EXCHANGE = 'store.retry.exchange';
export const DLQ_EXCHANGE = 'store.dlq.exchange';

export const INVENTORY_QUEUE = 'q.inventory.reserve';
export const PAYMENT_QUEUE = 'q.payment.process';
export const PAYMENT_RETRY_QUEUE = 'q.payment.retry';
export const COMPENSATION_QUEUE = 'q.inventory.compensate';
export const NOTIFICATIONS_QUEUE = 'q.notifications';
export const DLQ_QUEUE = 'q.orders.deadletter';

export async function initTopology() {
  const connection = await amqp.connect(RABBIT_URL);
  const channel = await connection.createChannel();

  // Exchanges
  await channel.assertExchange(MAIN_EXCHANGE, 'topic', { durable: true });
  await channel.assertExchange(RETRY_EXCHANGE, 'topic', { durable: true });
  await channel.assertExchange(DLQ_EXCHANGE, 'topic', { durable: true });

  // Dead Letter Queue
  await channel.assertQueue(DLQ_QUEUE, { durable: true });
  await channel.bindQueue(DLQ_QUEUE, DLQ_EXCHANGE, '#');

  // Inventory Queue
  await channel.assertQueue(INVENTORY_QUEUE, { durable: true });
  await channel.bindQueue(INVENTORY_QUEUE, MAIN_EXCHANGE, 'order.checkout');

  // Payment Queue
  await channel.assertQueue(PAYMENT_QUEUE, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': DLQ_EXCHANGE,
      'x-dead-letter-routing-key': 'order.dlq'
    }
  });
  await channel.bindQueue(PAYMENT_QUEUE, MAIN_EXCHANGE, 'order.inventory.reserved');

  // Payment Retry Queue (4-second TTL delay back to Payment Queue)
  await channel.assertQueue(PAYMENT_RETRY_QUEUE, {
    durable: true,
    arguments: {
      'x-message-ttl': 4000,
      'x-dead-letter-exchange': MAIN_EXCHANGE,
      'x-dead-letter-routing-key': 'order.inventory.reserved'
    }
  });
  await channel.bindQueue(PAYMENT_RETRY_QUEUE, RETRY_EXCHANGE, 'order.payment.retry');

  // Compensation Queue (Restores stock if payment fails)
  await channel.assertQueue(COMPENSATION_QUEUE, { durable: true });
  await channel.bindQueue(COMPENSATION_QUEUE, MAIN_EXCHANGE, 'order.payment.failed');

  // Audit / Notification Queue
  await channel.assertQueue(NOTIFICATIONS_QUEUE, { durable: true });
  await channel.bindQueue(NOTIFICATIONS_QUEUE, MAIN_EXCHANGE, 'order.#');

  await channel.close();
  await connection.close();
}