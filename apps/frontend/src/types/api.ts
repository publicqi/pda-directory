export interface Seed {
  index: number;
  raw_hex: string;
  length: number;
  is_bump: boolean;
}

export interface PdaEntry {
  pda: string;
  program_id: string;
  seed_count: number;
  seeds: Seed[];
}

export interface ApiResponse {
  query?: {
    pda?: string;
    program_id?: string;
  };
  limit: number;
  offset?: number; // Optional for cursor-based pagination
  count: number;
  results: PdaEntry[];
  has_next?: boolean;
  has_previous?: boolean;
  next_offset?: number | null;
  previous_offset?: number;
  next_cursor?: string; // New: for cursor-based pagination
}
