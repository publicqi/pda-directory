import { useEffect, useState } from 'react';
import { useSearchParams } from 'react-router-dom';

import { API_BASE_URL } from '../config';
import Header from '../components/Header';
import PdaEntryCard from '../components/PdaEntryCard';
import { PdaEntry, ExploreResponse } from '../types/api';

const ExplorePage = () => {
    const [searchParams, setSearchParams] = useSearchParams();
    const offsetFromUrl = parseInt(searchParams.get('offset') ?? '0', 10);

    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [entries, setEntries] = useState<PdaEntry[]>([]);
    const [hasLoaded, setHasLoaded] = useState(false);
    const [lastResponse, setLastResponse] = useState<ExploreResponse | null>(null);

    useEffect(() => {
        const controller = new AbortController();
        const { signal } = controller;

        const fetchEntries = async (offset: number) => {
            setIsLoading(true);
            setError(null);

            try {
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

                const data: ExploreResponse = await response.json();
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

        void fetchEntries(offsetFromUrl);

        return () => {
            controller.abort();
        };
    }, [offsetFromUrl]);

    const handlePreviousPage = () => {
        if (lastResponse?.has_previous) {
            setSearchParams({ offset: String(lastResponse.previous_offset) });
        }
    };

    const handleNextPage = () => {
        if (lastResponse?.has_next) {
            setSearchParams({ offset: String(lastResponse.next_offset) });
        }
    };

    const currentPage = Math.floor(offsetFromUrl / 25) + 1;
    const startRecord = offsetFromUrl + 1;
    const endRecord = offsetFromUrl + (entries.length);

    return (
        <>
            <Header />
            <div className="app-container">
                <section className="explore-header">
                    <h1>Explore PDA Directory</h1>
                    <p className="label">Browse through the latest PDA entries in the directory</p>
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
                            Page {currentPage} • Showing {startRecord}-{endRecord} records
                        </span>
                        {lastResponse && (
                            <div className="pagination-controls">
                                <button
                                    type="button"
                                    onClick={handlePreviousPage}
                                    disabled={!lastResponse.has_previous}
                                    className="pagination-button"
                                >
                                    ← Previous
                                </button>
                                <button
                                    type="button"
                                    onClick={handleNextPage}
                                    disabled={!lastResponse.has_next}
                                    className="pagination-button"
                                >
                                    Next →
                                </button>
                            </div>
                        )}
                    </div>
                ) : null}

                {isLoading ? (
                    <div className="surface-card">
                        <p className="label">Fetching PDA data…</p>
                    </div>
                ) : null}

                {!isLoading && !error && hasLoaded && entries.length === 0 ? (
                    <div className="surface-card">
                        <h2>No entries found</h2>
                        <p className="value-mono">
                            No PDA entries found in the directory.
                        </p>
                    </div>
                ) : null}

                {!isLoading && !error
                    ? entries.map((entry) => (
                        <PdaEntryCard key={`${entry.program_id}-${entry.pda}`} entry={entry} />
                    ))
                    : null}

                {!error && hasLoaded && lastResponse && entries.length > 0 ? (
                    <div className="pagination-footer">
                        <div className="pagination-controls">
                            <button
                                type="button"
                                onClick={handlePreviousPage}
                                disabled={!lastResponse.has_previous}
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
                                disabled={!lastResponse.has_next}
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

export default ExplorePage;
