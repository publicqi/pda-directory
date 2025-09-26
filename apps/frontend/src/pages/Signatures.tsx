import { FormEvent, useEffect, useRef, useState } from 'react';
import { useSearchParams } from 'react-router-dom';

import { API_BASE_URL } from '../config';
import Header from '../components/Header';
import PdaEntryCard from '../components/PdaEntryCard';
import { PdaEntry, SearchResponse, ExploreResponse } from '../types/api';

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
  const [searchResponse, setSearchResponse] = useState<SearchResponse | null>(null);
  const [exploreResponse, setExploreResponse] = useState<ExploreResponse | null>(null);
  const [isSearchMode, setIsSearchMode] = useState(!!queryFromUrl);
  const isLoading = isSearching || isExploring;

  useEffect(() => {
    const controller = new AbortController();
    const { signal } = controller;

    const fetchSearchResults = async (pda: string) => {
      const response = await fetch(`${API_BASE_URL}/api/pda/query`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ pda }),
        signal,
      });
      if (!response.ok) {
        const message = await response.text();
        throw new Error(message || `Request failed with status ${response.status}`);
      }
      return response.json() as Promise<SearchResponse>;
    };

    const fetchExploreResults = async (offset: number) => {
      const response = await fetch(`${API_BASE_URL}/api/pda/list`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ offset }),
        signal,
      });
      if (!response.ok) {
        const message = await response.text();
        throw new Error(message || `Request failed with status ${response.status}`);
      }
      return response.json() as Promise<ExploreResponse>;
    };

    const fetchData = async () => {
      const trimmedQuery = queryFromUrl.trim();
      setError(null);
      setQuery(queryFromUrl);
      const isSearching = !!trimmedQuery;
      setIsSearchMode(isSearching);

      try {
        if (isSearching) {
          // Search mode
          setIsSearching(true);
          const data = await fetchSearchResults(trimmedQuery);
          setEntries(data.results);
          setSearchResponse(data);
          setExploreResponse(null);
        } else {
          // Explore mode
          setIsExploring(true);
          const data = await fetchExploreResults(offsetFromUrl);
          setEntries(data.results);
          setExploreResponse(data);
          setSearchResponse(null);
        }
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
  }, [queryFromUrl, offsetFromUrl]);

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
    if (exploreResponse?.has_previous) {
      setSearchParams({ offset: String(exploreResponse.previous_offset) });
    }
  };

  const handleNextPage = () => {
    if (exploreResponse?.has_next) {
      setSearchParams({ offset: String(exploreResponse.next_offset) });
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
                placeholder="PDA Address"
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
              {isSearchMode && searchResponse?.query ? (
                <>
                  <span className="label">Results for </span>
                  <span className="value-mono">"{searchResponse.query}"</span>
                </>
              ) : (
                <span className="label">
                  {`Page ${currentPage} • Showing ${startRecord}-${endRecord} records`}
                </span>
              )}
            </span>
            {!isSearchMode && exploreResponse && (
              <div className="pagination-controls">
                <button
                  type="button"
                  onClick={handlePreviousPage}
                  disabled={!exploreResponse.has_previous || isExploring}
                  className="pagination-button"
                >
                  ← Previous
                </button>
                <button
                  type="button"
                  onClick={handleNextPage}
                  disabled={!exploreResponse.has_next || isExploring}
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

        {!error && hasLoaded && !isSearchMode && exploreResponse && entries.length > 0 ? (
          <div className="pagination-footer">
            <div className="pagination-controls">
              <button
                type="button"
                onClick={handlePreviousPage}
                disabled={!exploreResponse.has_previous || isExploring}
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
                disabled={!exploreResponse.has_next || isExploring}
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
