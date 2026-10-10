import React, { useState, useEffect, useRef } from 'react';
import { ShoppingBag, RefreshCw, CheckCircle, XCircle, Clock } from 'lucide-react';

const API_BASE = 'http://localhost:4000/api';

export default function App() {
  const [stock, setStock] = useState(null);
  const [scenario, setScenario] = useState('success');
  const [loading, setLoading] = useState(false);
  const [currentOrder, setCurrentOrder] = useState(null);
  const [logs, setLogs] = useState([]);
  const [orderStatus, setOrderStatus] = useState(null);

  const eventSourceRef = useRef(null);

  const fetchStock = async () => {
    try {
      const res = await fetch(`${API_BASE}/products/stock`);
      const data = await res.json();
      setStock(data.stock);
    } catch (err) {
      console.error('Failed to load stock', err);
    }
  };

  useEffect(() => {
    fetchStock();
    const interval = setInterval(fetchStock, 3000);
    return () => clearInterval(interval);
  }, []);

  const addLog = (msg, color) => {
    setLogs((prev) => [...prev, { time: new Date().toLocaleTimeString(), msg, color }]);
  };

  const handleCheckout = async () => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
    }

    setLoading(true);
    setLogs([]);
    setOrderStatus('PROCESSING');

    // Generate Order ID upfront so SSE connects prior to message emission
    const orderId = 'ORD-' + Math.floor(1000 + Math.random() * 9000);
    const idempotencyKey = crypto.randomUUID();
    setCurrentOrder(orderId);

    // 1. Establish SSE Connection
    const eventSource = new EventSource(`${API_BASE}/orders/${orderId}/stream`);
    eventSourceRef.current = eventSource;

    eventSource.addEventListener('order.checkout', () => {
      addLog('1. Order Placed: Emitted order.checkout', 'text-sky-400');
    });

    eventSource.addEventListener('order.inventory.reserved', () => {
      addLog('2. Inventory Reserved: Stock deducted in Redis', 'text-indigo-400');
      fetchStock();
    });

    eventSource.addEventListener('order.inventory.failed', (e) => {
      const payload = JSON.parse(e.data);
      addLog(`Failed: ${payload.reason}`, 'text-rose-400');
      setOrderStatus('FAILED');
      setLoading(false);
      eventSource.close();
    });

    eventSource.addEventListener('order.payment.completed', () => {
      addLog('3. Payment Processed: Transaction confirmed', 'text-emerald-400');
      setOrderStatus('CONFIRMED');
      setLoading(false);
      fetchStock();
      eventSource.close();
    });

    eventSource.addEventListener('order.payment.failed', (e) => {
      const payload = JSON.parse(e.data);
      addLog(`3. Payment Failed (${payload.reason}) -> Compensating transaction triggered: Restocked in Redis`, 'text-rose-400');
      setOrderStatus('CANCELLED & RESTOCKED');
      setLoading(false);
      fetchStock();
      eventSource.close();
    });

    eventSource.onerror = () => {
      // Stream closed or error
    };

    // 2. Small delay to ensure SSE handshake has completed before worker processes
    await new Promise((r) => setTimeout(r, 100));

    // 3. Dispatch Order
    try {
      const res = await fetch(`${API_BASE}/checkout`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ orderId, qty: 1, scenario, idempotencyKey })
      });
      if (!res.ok) {
        throw new Error('Checkout request failed');
      }
    } catch (err) {
      console.error(err);
      addLog('Error initiating order request', 'text-rose-400');
      setOrderStatus('FAILED');
      setLoading(false);
      eventSource.close();
    }
  };

  return (
    <div className="min-h-screen bg-neutral-950 text-neutral-100 p-8 font-sans">
      <div className="max-w-4xl mx-auto space-y-8">
        <header className="border-b border-neutral-800 pb-6 flex justify-between items-end">
          <div>
            <h1 className="text-2xl font-bold tracking-tight text-indigo-400">RabbitMQ Micro-Checkout</h1>
            <p className="text-sm text-neutral-400 mt-1">Saga Orchestration, Redis Caching & Asynchronous DLQ Retries</p>
          </div>
          <div className="bg-neutral-900 border border-neutral-800 px-4 py-2 rounded-lg text-right">
            <span className="text-xs text-neutral-400 block uppercase tracking-wider">Cached Redis Stock</span>
            <span className="text-xl font-mono font-semibold text-emerald-400">
              {stock !== null ? `${stock} units` : 'Syncing...'}
            </span>
          </div>
        </header>

        <div className="bg-neutral-900/60 border border-neutral-800 rounded-xl p-6 grid grid-cols-1 md:grid-cols-3 gap-6 items-center">
          <div className="md:col-span-2 space-y-2">
            <h2 className="text-xl font-medium">Mechanical Keyboard Pro</h2>
            <p className="text-sm text-neutral-400">Asynchronous checkout pipeline. Simulates card declines, broker retries, and automated stock rollback.</p>
            <div className="text-2xl font-bold font-mono pt-2">$149.00</div>
          </div>

          <div className="space-y-4">
            <div>
              <label className="text-xs text-neutral-400 block mb-1.5 uppercase font-medium">Test Pipeline Mode</label>
              <select
                value={scenario}
                onChange={(e) => setScenario(e.target.value)}
                disabled={loading}
                className="w-full bg-neutral-950 border border-neutral-700 rounded-lg px-3 py-2 text-sm focus:outline-none focus:border-indigo-500"
              >
                <option value="success">Success (Happy Path)</option>
                <option value="timeout">Gateway Timeout (Retry 4s -&gt; DLQ)</option>
                <option value="decline">Card Declined (Saga Stock Revert)</option>
              </select>
            </div>

            <button
              onClick={handleCheckout}
              disabled={loading || stock <= 0}
              className={`w-full py-2.5 px-4 rounded-lg font-medium transition text-sm flex justify-center items-center gap-2 ${
                loading
                  ? 'bg-neutral-800 text-neutral-500 cursor-not-allowed'
                  : 'bg-indigo-600 hover:bg-indigo-500 text-white shadow-lg shadow-indigo-600/20'
              }`}
            >
              {loading ? 'Processing via Broker...' : 'Place Order Now'}
            </button>
          </div>
        </div>

        {currentOrder && (
          <div className="bg-neutral-900/40 border border-neutral-800 rounded-xl p-6 space-y-4">
            <div className="flex justify-between items-center border-b border-neutral-800/80 pb-3">
              <span className="font-mono text-sm text-neutral-300">Tracking: {currentOrder}</span>
              <span className={`text-xs px-2.5 py-1 rounded-full font-mono border ${
                orderStatus === 'CONFIRMED' ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20' :
                orderStatus === 'PROCESSING' ? 'bg-amber-500/10 text-amber-400 border-amber-500/20' :
                'bg-rose-500/10 text-rose-400 border-rose-500/20'
              }`}>
                {orderStatus}
              </span>
            </div>

            <div className="space-y-2 font-mono text-xs">
              {logs.map((log, i) => (
                <div key={i} className={`p-2 rounded bg-neutral-950/60 border border-neutral-800/60 ${log.color}`}>
                  <span className="text-neutral-500 mr-2">[{log.time}]</span>
                  {log.msg}
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}