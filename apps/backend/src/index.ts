import { serve } from '@hono/node-server';
import { Hono } from 'hono';

const app = new Hono();

app.get('/', (c) => c.json({ status: 'ok' }));

const port = Number(process.env.PORT) || 8787;

console.log(`Backend listening on http://localhost:${port}`);

serve({
  fetch: app.fetch,
  port,
});
