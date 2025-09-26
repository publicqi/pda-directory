import React, { useEffect, useState } from 'react';
import { API_BASE_URL } from '../config';
import { formatTimeAgo } from '../utils/timeFormat';

interface StatsData {
    totalEntries: number;
    lastUpdateTime: number;
    isHealthy: boolean;
}

const StatsDisplay: React.FC = () => {
    const [stats, setStats] = useState<StatsData>({
        totalEntries: 0,
        lastUpdateTime: 0, // Default to Unix epoch (1970)
        isHealthy: false,
    });
    const [isLoading, setIsLoading] = useState(true);

    useEffect(() => {
        const fetchStats = async () => {
            try {
                // Fetch all three endpoints in parallel
                const [totalEntriesResponse, lastUpdateResponse, healthResponse] = await Promise.all([
                    fetch(`${API_BASE_URL}/api/total_entries`),
                    fetch(`${API_BASE_URL}/api/last_update_time`),
                    fetch(`${API_BASE_URL}/api/healthz`),
                ]);

                let totalEntries = 0;
                let lastUpdateTime = 0;
                let isHealthy = false;

                // Handle total entries
                if (totalEntriesResponse.ok) {
                    const totalData = await totalEntriesResponse.json();
                    const rawTotal = totalData.totalEntries;
                    if (rawTotal !== null && rawTotal !== undefined) {
                        totalEntries = typeof rawTotal === 'string' ? parseInt(rawTotal, 10) : rawTotal;
                        if (isNaN(totalEntries)) {
                            totalEntries = 0;
                        }
                    }
                }

                // Handle last update time
                if (lastUpdateResponse.ok) {
                    const updateData = await lastUpdateResponse.json();
                    const rawTime = updateData.lastUpdateTime;
                    if (rawTime !== null && rawTime !== undefined) {
                        lastUpdateTime = typeof rawTime === 'string' ? parseInt(rawTime, 10) : rawTime;
                        if (isNaN(lastUpdateTime)) {
                            lastUpdateTime = 0;
                        }
                    }
                }

                // Handle health check
                if (healthResponse.ok) {
                    const healthData = await healthResponse.json();
                    isHealthy = healthData.status === 'ok';
                }

                setStats({ totalEntries, lastUpdateTime, isHealthy });
            } catch (error) {
                // On any error, keep defaults (0 entries, 1970 timestamp, unhealthy)
                console.warn('Failed to fetch stats:', error);
            } finally {
                setIsLoading(false);
            }
        };

        fetchStats();
    }, []);

    if (isLoading) {
        return (
            <div className="stats-display">
                <div className="stats-item">
                    <span className="stats-label">Loading...</span>
                </div>
            </div>
        );
    }

    return (
        <div className="stats-display">
            <div className="stats-item">
                <span className="stats-value">{stats.totalEntries.toLocaleString()}</span>
                <span className="stats-label">entries</span>
            </div>
            <div className="stats-item">
                <div className={`health-dot ${stats.isHealthy ? 'healthy' : 'unhealthy'}`}></div>
                <span className="stats-label">updated {formatTimeAgo(stats.lastUpdateTime)}</span>
            </div>
        </div>
    );
};

export default StatsDisplay;
