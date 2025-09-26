import type { KVNamespace } from '@cloudflare/workers-types';
import { Hono } from 'hono';

const LAST_UPDATE_KEY = 'last_update_time';

type Env = {
  Bindings: {
    PDA_LAST_UPDATE: KVNamespace;
  };
};

const app = new Hono<Env>();

app.get('/healthz', (c) => c.json({ status: 'ok' }));

app.get('/last_update_time', async (c) => {
  const kv = c.env.PDA_LAST_UPDATE;

  if (!kv) {
    // Surface configuration mistakes early instead of returning stale data.
    return c.json({ error: 'KV namespace PDA_LAST_UPDATE is not configured' }, 500);
  }

  const lastUpdate = await kv.get(LAST_UPDATE_KEY);

  if (!lastUpdate) {
    return c.json({ lastUpdateTime: null });
  }

  return c.json({ lastUpdateTime: lastUpdate });
});

export default app;
