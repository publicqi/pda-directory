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

export interface SearchResponse {
  query: string | null;
  limit: number;
  offset: number;
  count: number;
  results: PdaEntry[];
}
