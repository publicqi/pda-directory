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
      params.set('offset', String(apiResponse.previous_offset));
      setSearchParams(params);
    }
  };

  const handleNextPage = () => {
    if (apiResponse?.has_next) {
      const params = new URLSearchParams(searchParams);
      params.set('offset', String(apiResponse.next_offset));
      setSearchParams(params);
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

        {!error && hasLoaded && !isSearchMode && apiResponse && entries.length > 0 ? (
          <div className="pagination-footer">
            <PaginationControls
              apiResponse={apiResponse}
              isLoading={isExploring}
              onPreviousPage={handlePreviousPage}
              onNextPage={handleNextPage}
              currentPage={currentPage}
              variant="footer"
            />
          </div>
        ) : null}
      </div>
    </>
  );
};

export default SignaturesPage;
