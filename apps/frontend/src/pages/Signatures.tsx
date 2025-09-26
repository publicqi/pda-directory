import { FormEvent, useEffect, useRef, useState } from 'react';
import { useSearchParams } from 'react-router-dom';

import { API_BASE_URL } from '../config';
import Header from '../components/Header';
import PdaEntryCard from '../components/PdaEntryCard';
import { PdaEntry, SearchResponse } from '../types/api';

const DEFAULT_LIMIT = 25;

const SignaturesPage = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const queryFromUrl = searchParams.get('q') ?? '';
  const searchInputRef = useRef<HTMLInputElement>(null);

  const [query, setQuery] = useState(queryFromUrl);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [entries, setEntries] = useState<PdaEntry[]>([]);
  const [hasLoaded, setHasLoaded] = useState(false);
  const [lastResponse, setLastResponse] = useState<SearchResponse | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    const { signal } = controller;

    const fetchEntries = async (rawQuery: string) => {
      const trimmed = rawQuery.trim();
      setIsLoading(true);
      setError(null);

      try {
        if (!trimmed) {
          // If no query, don't make a request
          setEntries([]);
          setLastResponse(null);
          setHasLoaded(true);
          return;
        }

        const response = await fetch(`${API_BASE_URL}/api/pda/query`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ pda: trimmed }),
          signal,
        });
        if (!response.ok) {
          const message = await response.text();
          throw new Error(message || `Request failed with status ${response.status}`);
        }

        const data: SearchResponse = await response.json();
        setEntries(data.results);
        setLastResponse(data);
        setHasLoaded(true);
      } catch (caught) {
        if (caught instanceof Error && caught.name === 'AbortError') {
          // Fetch was cancelled, do nothing
          return;
        }
        const message = caught instanceof Error ? caught.message : 'Unknown error';
        setError(message);
        setEntries([]);
      } finally {
        if (!signal.aborted) {
          setIsLoading(false);
        }
      }
    };

    setQuery(queryFromUrl);
    void fetchEntries(queryFromUrl);

    return () => {
      controller.abort();
    };
  }, [queryFromUrl]);

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

  return (
    <>
      <Header />
      <div className="app-container">

        <section className="search-panel">
          <form className="search-box" onSubmit={handleSubmit}>
            <input
              ref={searchInputRef}
              type="text"
              value={query}
              placeholder="PDA Address"
              onChange={(event) => setQuery(event.target.value)}
              spellCheck={false}
            />
            <button type="submit" className="search-button" disabled={isLoading}>
              {isLoading ? 'Searching…' : 'Search'}
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
            <span className="label">
              {lastResponse?.query ? `Results for \u201c${lastResponse.query}\u201d` : 'Latest directory entries'}
            </span>
            <span className="label">
              Showing {lastResponse?.count ?? entries.length} / {lastResponse?.limit ?? DEFAULT_LIMIT} records
            </span>
          </div>
        ) : null}

        {isLoading ? (
          <div className="surface-card">
            <p className="label">Fetching PDA data…</p>
          </div>
        ) : null}

        {!isLoading && !error && hasLoaded && entries.length === 0 ? (
          <div className="surface-card">
            <h2>No results</h2>
            <p className="value-mono">
              Nothing matched your query. Double-check that the PDA address exists in the directory.
            </p>
          </div>
        ) : null}

        {!isLoading && !error
          ? entries.map((entry) => (
            <PdaEntryCard key={`${entry.program_id}-${entry.pda}`} entry={entry} />
          ))
          : null}
      </div>
    </>
  );
};

export default SignaturesPage;
