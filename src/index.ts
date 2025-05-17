import express from 'express';
import cors from 'cors';
import { Kafka } from 'kafkajs';
import WebSocket, { WebSocketServer } from 'ws';

const app = express();
app.use(cors());

const PORT = 4000;
const WS_PORT = 4001;

// --- Types ---
type CustomerEvent = {
  store_id: number;
  customers_in: number;
  customers_out: number;
  time_stamp: string; // ISO timestamp string
};

type HourlyStat = {
  in: number;
  out: number;
};

type HistoryRow = {
  hour: string;
  in: number;
  out: number;
};

// --- In-memory storage ---
const recentMessages: CustomerEvent[] = []; // last 100 live messages

// Hourly stats for last 24 hours
const hourlyStats: Record<string, HourlyStat> = {};

// Helper to get hour string from timestamp: e.g. "2025-05-16T14"
function getHourKey(timestamp: string): string {
  const d = new Date(timestamp);
  if (isNaN(d.getTime())) return '';
  return d.toISOString().slice(0, 13); // "YYYY-MM-DDTHH"
}

// Clean up stats older than 24h periodically
function cleanOldStats() {
  const now = new Date();
  for (const hour in hourlyStats) {
    const hourDate = new Date(hour);
    if (now.getTime() - hourDate.getTime() > 24 * 60 * 60 * 1000) {
      delete hourlyStats[hour];
    }
  }
}

// --- Kafka Consumer Setup ---
const kafka = new Kafka({
  clientId: 'customer-dashboard',
  brokers: ['localhost:9092'], // Change as per your kafka setup
});

const topic = 'customer-events';

async function startKafkaConsumer() {
  const consumer = kafka.consumer({ groupId: 'dashboard-group' });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      const data: CustomerEvent = JSON.parse(message.value.toString());

      // Add to recentMessages (max 100)
      recentMessages.unshift(data);
      if (recentMessages.length > 100) recentMessages.pop();

      // Update hourlyStats
      const hourKey = getHourKey(data.time_stamp);
      if (!hourlyStats[hourKey]) hourlyStats[hourKey] = { in: 0, out: 0 };
      hourlyStats[hourKey].in += data.customers_in;
      hourlyStats[hourKey].out += data.customers_out;

      // Broadcast to WebSocket clients
      wss.clients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
          client.send(JSON.stringify(data));
        }
      });

      cleanOldStats();
    },
  });
}

// --- REST API ---

// History endpoint: return last 24 hours hourly summary sorted ascending
app.get('/api/history', (req, res) => {
  const now = new Date();
  const result: HistoryRow[] = [];

  for (let i = 23; i >= 0; i--) {
    const d = new Date(now.getTime() - i * 60 * 60 * 1000);
    const hourKey = d.toISOString().slice(0, 13);
    const stat = hourlyStats[hourKey] || { in: 0, out: 0 };
    const hourStr = `${hourKey.replace('T', ' ')}:00`;
    result.push({ hour: hourStr, in: stat.in, out: stat.out });
  }

  res.json(result);
});

// Live messages REST endpoint
app.get('/api/live', (req, res) => {
  res.json(recentMessages);
});

const server = app.listen(PORT, () => {
  console.log(`HTTP server listening on http://localhost:${PORT}`);
});

// --- WebSocket Server ---

const wss = new WebSocketServer({ port: WS_PORT });

wss.on('connection', (ws) => {
  console.log('Client connected to WebSocket');
  // Optionally send recent messages on connection (last 10)
  ws.send(JSON.stringify(recentMessages.slice(0, 10)));
});

// Start Kafka consumer
startKafkaConsumer().catch((err) => {
  console.error('Kafka consumer failed:', err);
  process.exit(1);
});
