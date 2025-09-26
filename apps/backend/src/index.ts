import type { D1Database, Fetcher, KVNamespace } from '@cloudflare/workers-types';
import { base58_to_binary, binary_to_base58 } from 'base58-js';
import { Hono } from 'hono';
import { cors } from 'hono/cors';
import { HTTPException } from 'hono/http-exception';

const LAST_UPDATE_KEY = 'last_update_time';
const TOTAL_ENTRIES_KEY = 'total_entries';

const DEFAULT_LIMIT = 25;
const MAX_LIMIT = 50;

type Env = {
  Bindings: {
    pda_directory: D1Database;
    PDA_METADATA: KVNamespace;
    ASSETS: Fetcher;
    API_BASE_URL?: string;
  };
};

type Settings = {
  defaultLimit: number;
  maxLimit: number;
};

type PdaRecord = {
  pda: Uint8Array;
  programId: Uint8Array;
  seedBytes: Uint8Array;
};

type ListParams = {
  queryBytes: Uint8Array | null;
  limit: number;
  offset: number;
};

const isTypedArray = (value: unknown): value is ArrayBufferView => ArrayBuffer.isView(value);

function toUint8Array(value: unknown, fieldName: string): Uint8Array {
  if (value instanceof Uint8Array) {
    return new Uint8Array(value);
  }

  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }

  if (isTypedArray(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }

  if (typeof value === 'string') {
    const normalized = value.startsWith('0x') ? value.slice(2) : value;
    if (normalized.length % 2 !== 0 || /[^0-9a-fA-F]/.test(normalized)) {
      throw new Error(`Value for ${fieldName} is not a valid hex string`);
    }

    const bytes = new Uint8Array(normalized.length / 2);
    for (let i = 0; i < normalized.length; i += 2) {
      bytes[i / 2] = Number.parseInt(normalized.slice(i, i + 2), 16);
    }
    return bytes;
  }

  throw new Error(`Value for ${fieldName} is not bytes-like`);
}

function assertExactByteLength(value: Uint8Array, expected: number, fieldName: string): void {
  if (value.byteLength !== expected) {
    throw new Error(`${fieldName} must be ${expected} bytes`);
  }
}

function base58Decode(value: string): Uint8Array {
  try {
    return new Uint8Array(base58_to_binary(value));
  } catch (error) {
    throw new Error('Invalid Base58 value');
  }
}

function base58Encode(bytes: Uint8Array): string {
  return binary_to_base58(bytes);
}

function parseQuery(value: string | null | undefined): Uint8Array | null {
  if (value === null || value === undefined) {
    return null;
  }

  const candidate = value.trim();
  if (candidate.length === 0) {
    return null;
  }

  let decoded: Uint8Array;
  try {
    decoded = base58Decode(candidate);
  } catch (error) {
    throw new HTTPException(400, {
      message: 'Query must be a valid base58 address',
    });
  }

  if (decoded.byteLength !== 32) {
    throw new HTTPException(400, {
      message: 'Query must decode to exactly 32 bytes',
    });
  }

  return decoded;
}

function readUint32LE(view: Uint8Array, offset: number): number {
  if (offset + 4 > view.byteLength) {
    throw new Error('Seed blob truncated before length prefix');
  }

  return (
    view[offset] |
    (view[offset + 1] << 8) |
    (view[offset + 2] << 16) |
    (view[offset + 3] << 24)
  ) >>> 0;
}

function decodeSeedBlob(blob: ArrayBuffer | Uint8Array): Uint8Array[] {
  const view = blob instanceof Uint8Array ? blob : new Uint8Array(blob);
  const totalLength = view.byteLength;

  if (totalLength < 4) {
    throw new Error('Seed blob too short to contain seed count');
  }

  let offset = 0;
  const seedCount = readUint32LE(view, offset);
  offset += 4;

  if (offset + seedCount * 4 > totalLength) {
    throw new Error('Seed blob truncated before seed descriptors');
  }

  const seeds: Uint8Array[] = [];
  for (let index = 0; index < seedCount; index += 1) {
    const length = readUint32LE(view, offset);
    offset += 4;

    if (length < 0) {
      throw new Error('Seed length must be non-negative');
    }

    if (offset + length > totalLength) {
      throw new Error('Seed blob truncated during payload read');
    }

    seeds.push(view.slice(offset, offset + length));
    offset += length;
  }

  if (offset !== totalLength) {
    throw new Error('Seed blob has trailing data after decoding seeds');
  }

  if (seeds.length !== seedCount) {
    throw new Error('Seed blob reported seed count mismatch');
  }

  return seeds;
}

function toHex(value: Uint8Array): string {
  return `0x${Array.from(value, (byte) => byte.toString(16).padStart(2, '0')).join('')}`;
}

function getSettings(): Settings {
  return {
    defaultLimit: DEFAULT_LIMIT,
    maxLimit: MAX_LIMIT,
  };
}

function sliceBuffer(bytes: Uint8Array): ArrayBuffer {
  const clone = new Uint8Array(bytes.byteLength);
  clone.set(bytes);
  return clone.buffer;
}

async function fetchPdas(
  db: D1Database,
  { queryBytes, limit, offset }: ListParams,
): Promise<PdaRecord[]> {
  let sql = 'SELECT pda, program_id, seed_bytes FROM pda_registry ';
  const params: unknown[] = [];

  if (queryBytes) {
    sql += 'WHERE pda = ? OR program_id = ? ';
    const buffer = sliceBuffer(queryBytes);
    params.push(buffer, sliceBuffer(queryBytes));
  }

  sql += 'ORDER BY pda LIMIT ? OFFSET ?';
  params.push(limit, offset);

  const statement = db.prepare(sql).bind(...params);
  const response = await statement.all<Record<string, unknown>>();

  if (!response.success || !response.results) {
    return [];
  }

  return response.results.map((row) => {
    const pda = toUint8Array(row.pda, 'pda');
    const programId = toUint8Array(row.program_id, 'program_id');
    const seedBytes = toUint8Array(row.seed_bytes, 'seed_bytes');

    assertExactByteLength(pda, 32, 'pda');
    assertExactByteLength(programId, 32, 'program_id');

    return {
      pda,
      programId,
      seedBytes,
    } satisfies PdaRecord;
  });
}

function resolveLimit(rawLimit: string | undefined | null, settings: Settings): number {
  if (rawLimit === undefined || rawLimit === null) {
    return settings.defaultLimit;
  }

  const parsed = Number.parseInt(rawLimit, 10);
  if (!Number.isInteger(parsed) || parsed < 1) {
    throw new HTTPException(400, { message: 'limit must be a positive integer' });
  }

  return Math.min(parsed, settings.maxLimit);
}

function resolveOffset(rawOffset: string | undefined | null): number {
  if (rawOffset === undefined || rawOffset === null) {
    return 0;
  }

  const parsed = Number.parseInt(rawOffset, 10);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new HTTPException(400, { message: 'offset must be a non-negative integer' });
  }

  return parsed;
}

export const createApp = (): Hono<Env> => {
  const app = new Hono<Env>();

  app.onError((err, c) => {
    if (err instanceof HTTPException) {
      const status = 'status' in err ? err.status : 500;
      const message = err.message || 'Request failed';
      return c.json({ error: message }, status);
    }

    console.error('Unhandled error in PDA Directory API', err);
    return c.json({ error: 'Internal Server Error' }, 500);
  });

  app.use('/api/*', cors({ origin: '*', allowMethods: ['GET'], allowHeaders: ['Content-Type'] }));

  app.use('*', async (c, next) => {
    const path = c.req.path;
    if (path.startsWith('/api')) {
      await next();
      return;
    }

    if (c.env.ASSETS && typeof c.env.ASSETS.fetch === 'function') {
      return c.env.ASSETS.fetch(c.req.raw);
    }

    return c.json({ error: 'Not Found' }, 404);
  });

  app.get('/api/healthz', (c) => c.json({ status: 'ok' }));

  app.get('/api/last_update_time', async (c) => {
    const kv = c.env.PDA_METADATA;
    if (!kv) {
      return c.json({ error: 'KV namespace PDA_METADATA is not configured' }, 500);
    }

    const lastUpdate = await kv.get(LAST_UPDATE_KEY);
    return c.json({ lastUpdateTime: lastUpdate ?? null });
  });

  app.get('/api/total_entries', async (c) => {
    const kv = c.env.PDA_METADATA;
    if (!kv) {
      return c.json({ error: 'KV namespace PDA_METADATA is not configured' }, 500);
    }

    const totalEntries = await kv.get(TOTAL_ENTRIES_KEY);
    return c.json({ totalEntries: totalEntries ?? null });

  });

  app.get('/api/pdas', async (c) => {
    const settings = getSettings();

    const qRaw = c.req.query('q');
    const limitRaw = c.req.query('limit');
    const offsetRaw = c.req.query('offset');

    const limit = resolveLimit(limitRaw, settings);
    const offset = resolveOffset(offsetRaw);

    const queryBytes = parseQuery(qRaw);

    const database = c.env.pda_directory;
    if (!database) {
      throw new HTTPException(500, { message: 'Database binding pda_directory is not configured' });
    }

    let rows: PdaRecord[];
    try {
      rows = await fetchPdas(database, { queryBytes, limit, offset });
    } catch (error) {
      console.error('Failed to fetch PDAs', error);
      throw new HTTPException(500, { message: 'Failed to read from PDA directory' });
    }

    const entries = rows.map((row) => {
      const seedsRaw = decodeSeedBlob(row.seedBytes);
      const seeds = seedsRaw.map((seed, index) => ({
        index,
        raw_hex: toHex(seed),
        length: seed.length,
        is_bump: seedsRaw.length > 0 && index === seedsRaw.length - 1,
      }));

      return {
        pda: base58Encode(row.pda),
        program_id: base58Encode(row.programId),
        seed_count: seedsRaw.length,
        seeds,
      };
    });

    return c.json({
      query: qRaw ?? null,
      limit,
      offset,
      count: entries.length,
      results: entries,
    });
  });

  app.all('/api/*', (c) => c.json({ error: 'Invalid request' }, 400));

  return app;
};

const app = createApp();

export { base58Decode, base58Encode, decodeSeedBlob, fetchPdas, parseQuery, resolveLimit, resolveOffset };
export type { Env, PdaRecord };

export default app;
