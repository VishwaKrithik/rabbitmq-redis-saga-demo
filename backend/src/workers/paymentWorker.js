import amqp from 'amqplib';
import { RABBIT_URL, MAIN_EXCHANGE, RETRY_EXCHANGE, PAYMENT_QUEUE } from '../config/rabbit.js';

const MAX_RETRIES = 2;

async function start() {
  const conn = await amqp.connect(RABBIT_URL);
  const ch = await conn.createChannel();
  ch.prefetch(1);

  console.log('[Worker: Payment] Online.');

  ch.consume(PAYMENT_QUEUE, async (msg) => {
    if (!msg) return;
    const order = JSON.parse(msg.content.toString());
    const headers = msg.properties.headers || {};
    const retryCount = headers['x-retry-count'] || 0;

    console.log(`[Payment] Processing ${order.orderId} (Attempt ${retryCount + 1}/${MAX_RETRIES + 1}) - Mode: ${order.scenario}`);

    if (order.scenario === 'decline') {
      console.error(`[Payment] Card declined for ${order.orderId}. Triggering Saga failure.`);
      ch.publish(MAIN_EXCHANGE, 'order.payment.failed', Buffer.from(JSON.stringify({ ...order, reason: 'Card Declined' })), { persistent: true });
      ch.ack(msg);
      return;
    }

    if (order.scenario === 'timeout' && retryCount < MAX_RETRIES) {
      console.warn(`[Payment] Network timeout on ${order.orderId}. Queuing to Retry Queue...`);
      ch.publish(RETRY_EXCHANGE, 'order.payment.retry', msg.content, {
        persistent: true,
        headers: { ...headers, 'x-retry-count': retryCount + 1 }
      });
      ch.ack(msg);
      return;
    }

    if (order.scenario === 'timeout' && retryCount >= MAX_RETRIES) {
      console.error(`[Payment] Retries exhausted for ${order.orderId}. Dead-lettering and failing order.`);
      ch.publish(MAIN_EXCHANGE, 'order.payment.failed', Buffer.from(JSON.stringify({ ...order, reason: 'Gateway Timeout (Exhausted Retries)' })), { persistent: true });
      ch.nack(msg, false, false); // NACK sends to DLQ
      return;
    }

    // Success path
    console.log(`[Payment] Payment successful for ${order.orderId}`);
    ch.publish(MAIN_EXCHANGE, 'order.payment.completed', Buffer.from(JSON.stringify(order)), { persistent: true });
    ch.ack(msg);
  });
}

start().catch(console.error);