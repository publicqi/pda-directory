import React, { useEffect, useState } from 'react';
import { formatTimeAgo } from '../utils/timeFormat';
import { fetchStats, StatsData } from '../utils/statsApi';

const StatsDisplay: React.FC = () => {
    const [stats, setStats] = useState<StatsData>({
        totalEntries: 0,
        lastUpdateTime: 0, // Default to Unix epoch (1970)
        isHealthy: false,
    });
    const [isLoading, setIsLoading] = useState(true);

    useEffect(() => {
        const loadStats = async () => {
            try {
                const statsData = await fetchStats();
                setStats(statsData);
            } catch (error) {
                // On any error, keep defaults (0 entries, 1970 timestamp, unhealthy)
                console.warn('Failed to fetch stats:', error);
            } finally {
                setIsLoading(false);
            }
        };

        loadStats();
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
