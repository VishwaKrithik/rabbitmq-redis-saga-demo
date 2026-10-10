# Event-Driven Micro-Checkout

A minimal event-driven checkout system demonstrating **RabbitMQ** (Saga pattern, DLQ, retries), **Redis** (stock cache & idempotency), **Express** (SSE streams), and **React**.

---

## Tech Stack

- **Backend:** Node.js, Express, RabbitMQ (`amqplib`), Redis (`redis`)
- **Frontend:** React (Vite), Tailwind CSS, Server-Sent Events (SSE)
- **Infra:** Docker Compose (RabbitMQ 3 Management + Redis 7)

---

## Quick Start

### 1. Start Infrastructure
```bash
docker compose up -d

```

### 2. Install Dependencies

```bash
# Backend
cd backend && npm install

# Frontend
cd ../frontend && npm install

```

### 3. Run the Services

Open separate terminal tabs:

```bash
# Terminal 1: API Server
cd backend && npm run server

# Terminal 2: Inventory Worker
cd backend && npm run worker:inventory

# Terminal 3: Payment Worker
cd backend && npm run worker:payment

# Terminal 4: Notification Worker
cd backend && npm run worker:notification

# Terminal 5: Frontend UI
cd frontend && npm run dev

```

* **Frontend App:** http://localhost:5173
* **RabbitMQ Dashboard:** http://localhost:15672 (`guest` / `guest`)

---

## How It Works

1. **Place Order (`POST /api/checkout`):**
* Checks Redis for duplicate requests (idempotency key).
* Returns an immediate `202 Accepted` response.
* Publishes `order.checkout` to RabbitMQ.


2. **Reserve Inventory (`inventoryWorker`):**
* Decrements stock atomically in Redis (`DECRBY`).
* If stock is available: emits `order.inventory.reserved`.
* If out of stock: emits `order.inventory.failed`.


3. **Process Payment (`paymentWorker`):**
* Listens for reserved inventory.
* **Success:** Emits `order.payment.completed`.
* **Decline:** Emits `order.payment.failed`. The inventory worker catches this on the compensation queue and restores stock (`INCRBY`).
* **Timeout:** Routes the message to a 4-second TTL retry queue. After 2 failed retries, sends the message to the Dead Letter Queue (`q.orders.deadletter`) and rolls back stock.


4. **Live UI Updates:**
* The frontend connects to an SSE endpoint (`/api/orders/:id/stream`) and displays the real-time event logs as they happen.



---

## Test Scenarios (via UI)

| Scenario | What Happens |
| --- | --- |
| **Success** | Stock deducted $\to$ Payment confirmed $\to$ Order marked `CONFIRMED`. |
| **Card Declined** | Stock deducted $\to$ Payment fails $\to$ Saga rolls back stock $\to$ Order marked `CANCELLED`. |
| **Gateway Timeout** | Payment retries twice after 4s delays $\to$ Message moves to DLQ $\to$ Stock rolls back. |

```
