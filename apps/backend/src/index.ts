import type { Fetcher, KVNamespace } from '@cloudflare/workers-types';
import { Hono } from 'hono';

const LAST_UPDATE_KEY = 'last_update_time';

type Env = {
  Bindings: {
    PDA_LAST_UPDATE: KVNamespace;
    ASSETS: Fetcher;
  };
};

const app = new Hono<Env>();

// Serve the frontend for non-API requests.
app.use('*', async (c, next) => {
  const url = new URL(c.req.url);
  if (url.hostname.startsWith('api.')) {
    await next();
  } else {
    return c.env.ASSETS.fetch(c.req.raw);
  }
});

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
