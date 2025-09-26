import type { D1Database, Fetcher, RateLimit } from '@cloudflare/workers-types';
import { base58_to_binary, binary_to_base58 } from 'base58-js';
import { Hono } from 'hono';
import { cors } from 'hono/cors';
import { HTTPException } from 'hono/http-exception';

type Env = {
  Bindings: {
    pda_directory: D1Database;
    ASSETS: Fetcher;
    API_BASE_URL?: string;
    PDA_DIRECTORY_RATE_LIMITER: RateLimit;
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
  pdaBytes: Uint8Array | null;
  programIdBytes: Uint8Array | null;
  limit: number;
  offset: number;
};

// D1 returns BLOBs as Array<number> on reads. Convert directly.
function fromD1Blob(value: unknown, fieldName: string): Uint8Array {
  if (Array.isArray(value)) return Uint8Array.from(value);
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  throw new Error(`${fieldName} must be a D1 BLOB (Array<number>).`);
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

function decodeSeedBlob(view: Uint8Array): Uint8Array[] {
  const totalLength = view.byteLength;
  if (totalLength < 4) throw new Error('Seed blob too short to contain seed count');

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

    if (offset + length > totalLength) throw new Error('Seed blob truncated during payload read');

    seeds.push(view.slice(offset, offset + length));
    offset += length;
  }

  if (offset !== totalLength) throw new Error('Seed blob has trailing data after decoding seeds');
  if (seeds.length !== seedCount) throw new Error('Seed blob reported seed count mismatch');

  return seeds;
}

function toHex(value: Uint8Array): string {
  return `0x${Array.from(value, (byte) => byte.toString(16).padStart(2, '0')).join('')}`;
}


function sliceBuffer(bytes: Uint8Array): ArrayBuffer {
  const clone = new Uint8Array(bytes.byteLength);
  clone.set(bytes);
  return clone.buffer;
}

async function fetchPdas(
  db: D1Database,
  { pdaBytes, programIdBytes, limit, offset }: ListParams,
): Promise<PdaRecord[]> {
  let sql = 'SELECT pda, program_id, seed_bytes FROM pda_registry ';
  const params: unknown[] = [];

  if (pdaBytes) {
    sql += 'WHERE pda = ? ';
    const buffer = sliceBuffer(pdaBytes);
    params.push(buffer);
  } else if (programIdBytes) {
    const hexByte = programIdBytes[0].toString(16).padStart(2, '0').toUpperCase();
    sql += `WHERE program_id = ? AND substr(program_id, 1, 1) = X'${hexByte}' `;
    const buffer = sliceBuffer(programIdBytes);
    params.push(buffer);
  }

  sql += 'ORDER BY pda LIMIT ? OFFSET ?';
  params.push(limit, offset);

  const statement = db.prepare(sql).bind(...params);
  type Row = {
    pda: number[] | Uint8Array | ArrayBuffer;
    program_id: number[] | Uint8Array | ArrayBuffer;
    seed_bytes: number[] | Uint8Array | ArrayBuffer;
  };

  const response = await statement.all<Row>();

  if (!response.success || !response.results) {
    return [];
  }

  return response.results.map((row) => {
    const pda = fromD1Blob(row.pda, 'pda');
    const programId = fromD1Blob(row.program_id, 'program_id');
    const seedBytes = fromD1Blob(row.seed_bytes, 'seed_bytes');

    assertExactByteLength(pda, 32, 'pda');
    assertExactByteLength(programId, 32, 'program_id');

    return { pda, programId, seedBytes } as PdaRecord;
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

  app.use('/api/*', cors({ origin: '*', allowMethods: ['GET', 'POST'], allowHeaders: ['Content-Type'] }));

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
    // SELECT last_insert_ts FROM _table_counts WHERE name = "pda_registry";
    const database = c.env.pda_directory;
    if (!database) {
      throw new HTTPException(500, { message: 'Database binding pda_directory is not configured' });
    }

    const lastUpdate = await database.prepare('SELECT last_insert_ts FROM _table_counts WHERE name = "pda_registry";').all();
    if (!lastUpdate.results) {
      throw new HTTPException(500, { message: 'Failed to fetch last update time' });
    }
    const lastUpdateTime = lastUpdate.results[0].last_insert_ts;
    return c.json({ lastUpdateTime: lastUpdateTime ?? null });
  });

  app.get('/api/total_entries', async (c) => {
    // SELECT n FROM _table_counts WHERE name = "pda_registry";
    const database = c.env.pda_directory;
    if (!database) {
      throw new HTTPException(500, { message: 'Database binding pda_directory is not configured' });
    }

    const totalEntries = await database.prepare('SELECT n FROM _table_counts WHERE name = "pda_registry";').all();
    if (!totalEntries.results) {
      throw new HTTPException(500, { message: 'Failed to fetch total entries' });
    }
    const totalEntriesCount = totalEntries.results[0].n;
    return c.json({ totalEntries: totalEntriesCount ?? null });

  });

  app.post('/api/pda/query', async (c) => {
    const limiter = c.env.PDA_DIRECTORY_RATE_LIMITER;
    if (!limiter) {
      throw new HTTPException(500, { message: 'Rate limit binding PDA_DIRECTORY_RATE_LIMITER is not configured' });
    }
    const { success } = await limiter.limit({ key: c.req.raw.headers.get('cf-connecting-ip') ?? '' });
    if (!success) {
      throw new HTTPException(429, { message: 'Rate limit exceeded (1 req/s)' });
    }

    const body = await c.req.json().catch(() => ({}));
    const { pda, program_id, limit: limitFromBody, offset: offsetFromBody } = body;

    if (pda && typeof pda !== 'string') {
      throw new HTTPException(400, { message: 'pda must be a string' });
    }
    if (program_id && typeof program_id !== 'string') {
      throw new HTTPException(400, { message: 'program_id must be a string' });
    }

    const pdaBytes = pda ? parseQuery(pda) : null;
    const programIdBytes = program_id ? parseQuery(program_id) : null;

    let limit: number;
    let offset: number;

    if (pdaBytes) {
      limit = 1;
      offset = 0;
    } else {
      const settings: Settings = { defaultLimit: 25, maxLimit: 50 };
      limit = resolveLimit(limitFromBody?.toString(), settings);
      offset = resolveOffset(offsetFromBody?.toString());
    }

    const database = c.env.pda_directory;
    if (!database) {
      throw new HTTPException(500, { message: 'Database binding pda_directory is not configured' });
    }

    let rows: PdaRecord[];
    try {
      rows = await fetchPdas(database, { pdaBytes, programIdBytes, limit, offset });
    } catch (error) {
      console.error('Failed to fetch PDAs', error);
      throw new HTTPException(500, { message: 'Failed to read from PDA directory. Maybe database is uploading... Try again later.' });
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

    const responsePayload: { [key: string]: unknown } = {
      limit,
      offset,
      count: entries.length,
      results: entries,
    };

    if (pda) {
      responsePayload.query = { pda };
    } else if (program_id) {
      responsePayload.query = { program_id };
    }

    if (!pdaBytes) {
      responsePayload.has_next = entries.length === limit;
      responsePayload.has_previous = offset > 0;
      responsePayload.next_offset = offset + limit;
      responsePayload.previous_offset = Math.max(0, offset - limit);
    }

    return c.json(responsePayload);
  });

  app.all('/api/*', (c) => c.json({ error: 'Invalid request' }, 400));

  return app;
};

const app = createApp();

export { base58Decode, base58Encode, decodeSeedBlob, fetchPdas, parseQuery, resolveLimit, resolveOffset };
export type { Env, PdaRecord };

export default app;
