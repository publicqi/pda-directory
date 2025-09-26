import { FormEvent, useEffect, useRef, useState } from 'react';
import { useSearchParams } from 'react-router-dom';

import { API_BASE_URL } from '../config';
import Header from '../components/Header';
import PdaEntryCard from '../components/PdaEntryCard';
import { PdaEntry, ApiResponse } from '../types/api';

// Solana API utility function
const checkIfAddressIsExecutable = async (address: string): Promise<boolean | null> => {
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
      return null;
    }

    const data = await response.json();

    // If there's an error in the response, return null
    if (data.error) {
      return null;
    }

    // If account doesn't exist, return null
    if (!data.result || !data.result.value) {
      return null;
    }

    // Check if the account is executable
    return data.result.value.executable === true;
  } catch (error) {
    console.warn('Error checking if address is executable:', error);
    return null;
  }
};

const SignaturesPage = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const queryFromUrl = searchParams.get('q') ?? '';
  const offsetFromUrl = parseInt(searchParams.get('offset') ?? '0', 10);
  const searchInputRef = useRef<HTMLInputElement>(null);

  const [query, setQuery] = useState(queryFromUrl);
  const [isSearching, setIsSearching] = useState(false);
  const [isExploring, setIsExploring] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [entries, setEntries] = useState<PdaEntry[]>([]);
  const [hasLoaded, setHasLoaded] = useState(false);
  const [apiResponse, setApiResponse] = useState<ApiResponse | null>(null);
  const isSearchMode = !!queryFromUrl;
  const isLoading = isSearching || isExploring;

  useEffect(() => {
    const controller = new AbortController();
    const { signal } = controller;

    const fetchPdas = async (pdaOrProgramId: string | null, offset: number, searchType: 'pda' | 'program_id') => {
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
    };

    const searchWithAutoDetection = async (address: string, offset: number): Promise<ApiResponse> => {
      // First, try to determine if the address is executable (program)
      const isExecutable = await checkIfAddressIsExecutable(address);

      if (isExecutable === true) {
        // Address is executable, search as program_id
        return await fetchPdas(address, offset, 'program_id');
      } else if (isExecutable === false) {
        // Address is not executable, search as pda
        return await fetchPdas(address, offset, 'pda');
      } else {
        // API call failed or returned null, try both approaches
        // First try PDA search
        try {
          const pdaResults = await fetchPdas(address, offset, 'pda');
          if (pdaResults.results.length > 0) {
            return pdaResults;
          }
        } catch (error) {
          // PDA search failed, continue to program_id search
        }

        // If PDA search returned no results or failed, try program_id search
        return await fetchPdas(address, offset, 'program_id');
      }
    };

    const fetchData = async () => {
      const trimmedQuery = queryFromUrl.trim();
      setError(null);
      setQuery(queryFromUrl);

      try {
        if (isSearchMode) {
          setIsSearching(true);
        } else {
          setIsExploring(true);
        }

        let data: ApiResponse;
        if (isSearchMode && trimmedQuery) {
          // Use auto-detection for search mode
          data = await searchWithAutoDetection(trimmedQuery, offsetFromUrl);
        } else {
          // For exploration mode (no search query), use the original logic
          data = await fetchPdas(null, offsetFromUrl, 'pda');
        }

        setEntries(data.results);
        setApiResponse(data);
        setHasLoaded(true);
      } catch (caught) {
        if (caught instanceof Error && caught.name === 'AbortError') {
          return;
        }
        const message = caught instanceof Error ? caught.message : 'Unknown error';
        setError(message);
        setEntries([]);
      } finally {
        if (!signal.aborted) {
          setIsSearching(false);
          setIsExploring(false);
        }
      }
    };

    void fetchData();

    return () => {
      controller.abort();
    };
  }, [queryFromUrl, offsetFromUrl, isSearchMode]);

  // Auto-focus search input on page load
  useEffect(() => {
    if (searchInputRef.current) {
      searchInputRef.current.focus();
    }
  }, []);

  const handleSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const trimmed = query.trim();

    if (trimmed) {
      setSearchParams({ q: trimmed });
    } else {
      setSearchParams({});
    }
  };

  const handleClearSearch = () => {
    setQuery('');
    setSearchParams({});
  };

  const handlePreviousPage = () => {
    if (apiResponse?.has_previous) {
      setSearchParams({ offset: String(apiResponse.previous_offset) });
    }
  };

  const handleNextPage = () => {
    if (apiResponse?.has_next) {
      setSearchParams({ offset: String(apiResponse.next_offset) });
    }
  };

  const currentPage = Math.floor(offsetFromUrl / 25) + 1;
  const startRecord = offsetFromUrl + 1;
  const endRecord = offsetFromUrl + (entries.length);

  return (
    <>
      <Header />
      <div className="app-container">
        <section className="search-panel">
          <form className="search-box" onSubmit={handleSubmit}>
            <div className="search-input-container">
              <input
                ref={searchInputRef}
                type="text"
                value={query}
                placeholder="PDA/Program ID"
                onChange={(event) => setQuery(event.target.value)}
                spellCheck={false}
              />
              {query && (
                <button
                  type="button"
                  className="clear-search-button"
                  onClick={handleClearSearch}
                  title="Clear search"
                >
                  ×
                </button>
              )}
            </div>
            <button type="submit" className="search-button" disabled={isSearching}>
              {isSearching ? 'Searching…' : 'Search'}
            </button>
          </form>
        </section>

        {error ? (
          <div className="surface-card">
            <h2>Error</h2>
            <p className="value-mono">{error}</p>
          </div>
        ) : null}

        {!error && hasLoaded ? (
          <div className="results-meta">
            <span className="results-for-text">
              {isSearchMode && apiResponse?.query ? (
                <>
                  <span className="label">Results for </span>
                  <span className="value-mono">&quot;{apiResponse.query.pda || apiResponse.query.program_id}&quot;</span>
                </>
              ) : (
                <span className="label">
                  {`Page ${currentPage} • Showing ${startRecord}-${endRecord} records`}
                </span>
              )}
            </span>
            {!isSearchMode && apiResponse && (
              <div className="pagination-controls">
                <button
                  type="button"
                  onClick={handlePreviousPage}
                  disabled={!apiResponse.has_previous || isExploring}
                  className="pagination-button"
                >
                  ← Previous
                </button>
                <button
                  type="button"
                  onClick={handleNextPage}
                  disabled={!apiResponse.has_next || isExploring}
                  className="pagination-button"
                >
                  Next →
                </button>
              </div>
            )}
          </div>
        ) : null}

        <div className={`pda-entries-list ${isLoading ? 'loading' : ''}`}>
          {isLoading && entries.length === 0 ? (
            <div className="surface-card">
              <p className="label">Fetching PDA data…</p>
            </div>
          ) : null}

          {!isLoading && !error && hasLoaded && entries.length === 0 ? (
            <div className="surface-card">
              <h2>No results</h2>
              <p className="value-mono">
                {isSearchMode
                  ? 'Nothing matched your query'
                  : 'No PDA entries found in the directory.'}
              </p>
            </div>
          ) : null}

          {!error
            ? entries.map((entry) => (
              <PdaEntryCard key={`${entry.program_id}-${entry.pda}`} entry={entry} />
            ))
            : null}
        </div>

        {!error && hasLoaded && !isSearchMode && apiResponse && entries.length > 0 ? (
          <div className="pagination-footer">
            <div className="pagination-controls">
              <button
                type="button"
                onClick={handlePreviousPage}
                disabled={!apiResponse.has_previous || isExploring}
                className="pagination-button"
              >
                ← Previous Page
              </button>
              <span className="pagination-info">
                Page {currentPage}
              </span>
              <button
                type="button"
                onClick={handleNextPage}
                disabled={!apiResponse.has_next || isExploring}
                className="pagination-button"
              >
                Next Page →
              </button>
            </div>
          </div>
        ) : null}
      </div>
    </>
  );
};

export default SignaturesPage;
