import amqp from 'amqplib';
import { connectRedis } from '../config/redis.js';
import { RABBIT_URL, MAIN_EXCHANGE, INVENTORY_QUEUE, COMPENSATION_QUEUE } from '../config/rabbit.js';

async function start() {
  const redis = await connectRedis();
  
  // Seed initial stock in Redis if not present
  const currentStock = await redis.get('product:stock:prod_1');
  if (currentStock === null) {
    await redis.set('product:stock:prod_1', 15);
  }

  const conn = await amqp.connect(RABBIT_URL);
  const ch = await conn.createChannel();
  ch.prefetch(1);

  console.log('[Worker: Inventory] Online. Watching reserve & compensate channels.');

  // Stock Reservation
  ch.consume(INVENTORY_QUEUE, async (msg) => {
    if (!msg) return;
    const order = JSON.parse(msg.content.toString());

    // Atomic decrement in Redis
    const remaining = await redis.decrBy('product:stock:prod_1', order.qty);

    if (remaining >= 0) {
      console.log(`[Inventory] Allocated ${order.qty} items for ${order.orderId}. Remaining: ${remaining}`);
      ch.publish(MAIN_EXCHANGE, 'order.inventory.reserved', Buffer.from(JSON.stringify(order)), { persistent: true });
    } else {
      // Revert the decrement if stock was insufficient
      await redis.incrBy('product:stock:prod_1', order.qty);
      console.warn(`[Inventory] Insufficient stock for ${order.orderId}`);
      ch.publish(MAIN_EXCHANGE, 'order.inventory.failed', Buffer.from(JSON.stringify({ ...order, reason: 'Out of Stock' })), { persistent: true });
    }
    ch.ack(msg);
  });

  // Saga Compensation: Restore Inventory
  ch.consume(COMPENSATION_QUEUE, async (msg) => {
    if (!msg) return;
    const order = JSON.parse(msg.content.toString());
    const restored = await redis.incrBy('product:stock:prod_1', order.qty);
    console.log(`[Saga Compensation] Restored ${order.qty} items for ${order.orderId}. Current Stock: ${restored}`);
    ch.ack(msg);
  });
}

start().catch(console.error);