import amqp from 'amqplib';
import { RABBIT_URL, NOTIFICATIONS_QUEUE } from '../config/rabbit.js';

async function start() {
  const conn = await amqp.connect(RABBIT_URL);
  const ch = await conn.createChannel();
  ch.prefetch(1);

  console.log('[Worker: Notification] Listening for order events...');

  ch.consume(NOTIFICATIONS_QUEUE, (msg) => {
    if (!msg) return;
    const routingKey = msg.fields.routingKey;
    const data = JSON.parse(msg.content.toString());
    console.log(`[Audit Trail] ${routingKey.toUpperCase()} -> Order ID: ${data.orderId}`);
    ch.ack(msg);
  });
}

start().catch(console.error);