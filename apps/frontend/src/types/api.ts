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
  offset: number;
  count: number;
  results: PdaEntry[];
  has_next?: boolean;
  has_previous?: boolean;
  next_offset?: number;
  previous_offset?: number;
}
