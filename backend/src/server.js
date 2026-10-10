import express from 'express';
import cors from 'cors';
import amqp from 'amqplib';
import { connectRedis } from './config/redis.js';
import { RABBIT_URL, MAIN_EXCHANGE, initTopology } from './config/rabbit.js';

const app = express();
app.use(cors({ origin: 'http://localhost:5173' })); // Allow Vite React dev server
app.use(express.json());

let confirmChannel;
let redis;
const sseConnections = new Map(); // orderId -> Response object

// SSE Stream
app.get('/api/orders/:orderId/stream', (req, res) => {
  const { orderId } = req.params;

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  sseConnections.set(orderId, res);

  req.on('close', () => {
    sseConnections.delete(orderId);
  });
});

// Live Stock from Redis
app.get('/api/products/stock', async (req, res) => {
  const stock = await redis.get('product:stock:prod_1');
  res.json({ productId: 'prod_1', stock: parseInt(stock || '0', 10) });
});

// Checkout Endpoint
app.post('/api/checkout', async (req, res) => {
  const { scenario = 'success', qty = 1, idempotencyKey, orderId: reqOrderId } = req.body;

  // Use client-provided orderId to prevent SSE race condition
  const orderId = reqOrderId || ('ORD-' + Math.floor(1000 + Math.random() * 9000));

  // Idempotency check via Redis
  if (idempotencyKey) {
    const isProcessed = await redis.set(`idempotency:${idempotencyKey}`, '1', {
      NX: true,
      EX: 60 // 60s window
    });
    if (!isProcessed) {
      return res.status(409).json({ error: 'Duplicate transaction detected.' });
    }
  }

  const payload = {
    orderId,
    qty: parseInt(qty, 10),
    scenario,
    timestamp: new Date().toISOString()
  };

  confirmChannel.publish(
    MAIN_EXCHANGE,
    'order.checkout',
    Buffer.from(JSON.stringify(payload)),
    { persistent: true },
    (err) => {
      if (err) return res.status(500).json({ error: 'Message broker rejected payload' });
      res.status(202).json({
        message: 'Order accepted for asynchronous processing',
        orderId,
        status: 'PROCESSING'
      });
    }
  );
});

async function bootstrap() {
  await initTopology();
  redis = await connectRedis();

  const conn = await amqp.connect(RABBIT_URL);
  confirmChannel = await conn.createConfirmChannel();

  // SSE Broadcaster Queue
  const sseChan = await conn.createChannel();
  const q = await sseChan.assertQueue('', { exclusive: true });
  await sseChan.bindQueue(q.queue, MAIN_EXCHANGE, 'order.#');

  sseChan.consume(q.queue, (msg) => {
    if (!msg) return;
    const routingKey = msg.fields.routingKey;
    const payload = JSON.parse(msg.content.toString());
    const client = sseConnections.get(payload.orderId);

    if (client) {
      client.write(`event: ${routingKey}\ndata: ${JSON.stringify(payload)}\n\n`);
    }
    sseChan.ack(msg);
  });

  const PORT = 4000;
  app.listen(PORT, () => {
    console.log(`[API Server] Running on http://localhost:${PORT}`);
  });
}

bootstrap().catch(console.error);