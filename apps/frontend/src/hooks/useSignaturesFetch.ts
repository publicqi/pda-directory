import { useEffect, useReducer, useCallback } from 'react';
import { API_BASE_URL } from '../config';
import { ApiResponse } from '../types/api';

// State machine for fetch operations
type FetchState = 
  | { status: 'idle' }
  | { status: 'searching' }
  | { status: 'exploring' }
  | { status: 'success'; data: ApiResponse }
  | { status: 'error'; error: string };

type FetchAction =
  | { type: 'START_SEARCH' }
  | { type: 'START_EXPLORE' }
  | { type: 'SUCCESS'; payload: ApiResponse }
  | { type: 'ERROR'; payload: string }
  | { type: 'RESET' };

function fetchReducer(state: FetchState, action: FetchAction): FetchState {
  switch (action.type) {
    case 'START_SEARCH':
      return { status: 'searching' };
    case 'START_EXPLORE':
      return { status: 'exploring' };
    case 'SUCCESS':
      return { status: 'success', data: action.payload };
    case 'ERROR':
      return { status: 'error', error: action.payload };
    case 'RESET':
      return { status: 'idle' };
    default:
      return state;
  }
}

// Cache for executable checks to avoid redundant RPC calls
const executableCache = new Map<string, { result: boolean | null; timestamp: number }>();
const CACHE_TTL = 5 * 60 * 1000; // 5 minutes

const checkIfAddressIsExecutable = async (address: string): Promise<boolean | null> => {
  const now = Date.now();
  const cached = executableCache.get(address);
  
  if (cached && (now - cached.timestamp) < CACHE_TTL) {
    return cached.result;
  }

  try {
    const response = await fetch('https://api.mainnet-beta.solana.com', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'getAccountInfo',
        params: [
          address,
          {
            commitment: 'finalized',
            encoding: 'base58',
            dataSlice: {
              length: 0
            }
          }
        ]
      })
    });

    if (!response.ok) {
      executableCache.set(address, { result: null, timestamp: now });
      return null;
    }

    const data = await response.json();

    if (data.error || !data.result || !data.result.value) {
      executableCache.set(address, { result: null, timestamp: now });
      return null;
    }

    const result = data.result.value.executable === true;
    executableCache.set(address, { result, timestamp: now });
    return result;
  } catch (error) {
    console.warn('Error checking if address is executable:', error);
    executableCache.set(address, { result: null, timestamp: now });
    return null;
  }
};

interface UseSignaturesFetchParams {
  query: string | null;
  offset: number;
  isSearchMode: boolean;
}

export function useSignaturesFetch({ query, offset, isSearchMode }: UseSignaturesFetchParams) {
  const [state, dispatch] = useReducer(fetchReducer, { status: 'idle' });

  const fetchPdas = useCallback(async (
    pdaOrProgramId: string | null, 
    offset: number, 
    searchType: 'pda' | 'program_id',
    signal: AbortSignal
  ): Promise<ApiResponse> => {
    const body: { [key: string]: string | number } = { offset };
    if (pdaOrProgramId) {
      body[searchType] = pdaOrProgramId;
    }

    const response = await fetch(`${API_BASE_URL}/api/pda/query`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
      signal,
    });

    if (!response.ok) {
      const message = await response.text();
      throw new Error(message || `Request failed with status ${response.status}`);
    }

    return response.json() as Promise<ApiResponse>;
  }, []);

  const searchWithAutoDetection = useCallback(async (
    address: string, 
    offset: number, 
    signal: AbortSignal
  ): Promise<ApiResponse> => {
    const isExecutable = await checkIfAddressIsExecutable(address);

    if (isExecutable === true) {
      return await fetchPdas(address, offset, 'program_id', signal);
    } else if (isExecutable === false) {
      return await fetchPdas(address, offset, 'pda', signal);
    } else {
      // Try PDA first, then program_id
      try {
        const pdaResults = await fetchPdas(address, offset, 'pda', signal);
        if (pdaResults.results.length > 0) {
          return pdaResults;
        }
      } catch (error) {
        // Continue to program_id search
      }

      return await fetchPdas(address, offset, 'program_id', signal);
    }
  }, [fetchPdas]);

  useEffect(() => {
    const controller = new AbortController();
    const { signal } = controller;

    const fetchData = async () => {
      const trimmedQuery = query?.trim();
      
      try {
        if (isSearchMode && trimmedQuery) {
          dispatch({ type: 'START_SEARCH' });
          const data = await searchWithAutoDetection(trimmedQuery, offset, signal);
          dispatch({ type: 'SUCCESS', payload: data });
        } else {
          dispatch({ type: 'START_EXPLORE' });
          const data = await fetchPdas(null, offset, 'pda', signal);
          dispatch({ type: 'SUCCESS', payload: data });
        }
      } catch (caught) {
        if (caught instanceof Error && caught.name === 'AbortError') {
          return;
        }
        const message = caught instanceof Error ? caught.message : 'Unknown error';
        dispatch({ type: 'ERROR', payload: message });
      }
    };

    void fetchData();

    return () => {
      controller.abort();
    };
  }, [query, offset, isSearchMode, searchWithAutoDetection, fetchPdas]);

  return {
    state,
    isLoading: state.status === 'searching' || state.status === 'exploring',
    isSearching: state.status === 'searching',
    isExploring: state.status === 'exploring',
    hasLoaded: state.status === 'success' || state.status === 'error',
    entries: state.status === 'success' ? state.data.results : [],
    apiResponse: state.status === 'success' ? state.data : null,
    error: state.status === 'error' ? state.error : null,
  };
}
