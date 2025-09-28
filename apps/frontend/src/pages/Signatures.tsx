import { FormEvent, useEffect, useRef, useState } from 'react';
import { useSearchParams } from 'react-router-dom';

import Header from '../components/Header';
import PdaEntryCard from '../components/PdaEntryCard';
import PaginationControls from '../components/PaginationControls';
import { useSignaturesFetch } from '../hooks/useSignaturesFetch';

const SignaturesPage = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const queryFromUrl = searchParams.get('q') ?? '';
  const offsetFromUrl = parseInt(searchParams.get('offset') ?? '0', 10);
  const cursorFromUrl = searchParams.get('cursor') ?? null;
  const searchInputRef = useRef<HTMLInputElement>(null);

  const [query, setQuery] = useState(queryFromUrl);
  const isSearchMode = !!queryFromUrl;

  const {
    isLoading,
    isSearching,
    isExploring,
    hasLoaded,
    entries,
    apiResponse,
    error,
  } = useSignaturesFetch({
    query: queryFromUrl,
    offset: offsetFromUrl,
    cursor: cursorFromUrl,
    isSearchMode,
  });


  // Sync local query state with URL changes (browser back/forward, bookmarks)
  useEffect(() => {
    setQuery(queryFromUrl);
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
      setQuery(trimmed); // Update local state to show trimmed value immediately
    } else {
      setSearchParams({});
      setQuery(''); // Clear local state immediately
    }
  };

  const handleClearSearch = () => {
    setQuery('');
    setSearchParams({});
  };

  const handlePreviousPage = () => {
    if (apiResponse?.has_previous) {
      const params = new URLSearchParams(searchParams);
      // For previous page, always use offset-based pagination
      if (apiResponse.previous_offset !== undefined) {
        params.set('offset', String(apiResponse.previous_offset));
        params.delete('cursor'); // Remove cursor when going back to offset
      }
      setSearchParams(params);
    }
  };

  const handleNextPage = () => {
    if (apiResponse?.has_next) {
      const params = new URLSearchParams(searchParams);

      // Prefer cursor pagination for better performance on deep pages
      if (apiResponse.next_cursor) {
        params.set('cursor', apiResponse.next_cursor);
        params.delete('offset'); // Remove offset when using cursor
      } else if (apiResponse.next_offset !== null && apiResponse.next_offset !== undefined) {
        params.set('offset', String(apiResponse.next_offset));
        params.delete('cursor');
      }
      setSearchParams(params);
    }
  };

  // Calculate page info based on whether we're using cursor or offset
  const isUsingCursor = cursorFromUrl !== null;
  const currentPage = isUsingCursor ? null : Math.floor(offsetFromUrl / 25) + 1;
  const startRecord = isUsingCursor ? null : offsetFromUrl + 1;
  const endRecord = isUsingCursor ? null : offsetFromUrl + (entries.length);

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
                  {isUsingCursor
                    ? `Showing ${entries.length} records (cursor-based pagination)`
                    : `Page ${currentPage} • Showing ${startRecord}-${endRecord} records`
                  }
                </span>
              )}
            </span>
            {apiResponse && (
              <PaginationControls
                apiResponse={apiResponse}
                isLoading={isExploring}
                onPreviousPage={handlePreviousPage}
                onNextPage={handleNextPage}
                variant="inline"
              />
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

        {!error && hasLoaded && apiResponse && entries.length > 0 ? (
          <div className="pagination-footer">
            <PaginationControls
              apiResponse={apiResponse}
              isLoading={isExploring}
              onPreviousPage={handlePreviousPage}
              onNextPage={handleNextPage}
              currentPage={currentPage ?? undefined}
              variant="footer"
            />
          </div>
        ) : null}
      </div>
    </>
  );
};

export default SignaturesPage;
