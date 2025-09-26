import { API_BASE_URL } from '../config';

export interface StatsData {
    totalEntries: number;
    lastUpdateTime: number;
    isHealthy: boolean;
}

interface TotalEntriesResponse {
    totalEntries: string | number;
}

interface LastUpdateResponse {
    lastUpdateTime: string | number;
}

interface HealthResponse {
    status: string;
}

interface ApiEndpointHandler<T, R> {
    url: string;
    parser: (data: R) => T;
    defaultValue: T;
}

const parseNumber = (value: string | number | null | undefined): number => {
    if (value === null || value === undefined) return 0;
    const parsed = typeof value === 'string' ? parseInt(value, 10) : value;
    return isNaN(parsed) ? 0 : parsed;
};

const apiHandlers = {
    totalEntries: {
        url: `${API_BASE_URL}/api/total_entries`,
        parser: (data: TotalEntriesResponse) => parseNumber(data.totalEntries),
        defaultValue: 0,
    } as ApiEndpointHandler<number, TotalEntriesResponse>,

    lastUpdateTime: {
        url: `${API_BASE_URL}/api/last_update_time`,
        parser: (data: LastUpdateResponse) => parseNumber(data.lastUpdateTime),
        defaultValue: 0,
    } as ApiEndpointHandler<number, LastUpdateResponse>,

    isHealthy: {
        url: `${API_BASE_URL}/api/healthz`,
        parser: (data: HealthResponse) => data.status === 'ok',
        defaultValue: false,
    } as ApiEndpointHandler<boolean, HealthResponse>,
};

async function fetchEndpoint<T, R>(handler: ApiEndpointHandler<T, R>): Promise<T> {
    try {
        const response = await fetch(handler.url);
        if (!response.ok) {
            return handler.defaultValue;
        }
        const data = await response.json() as R;
        return handler.parser(data);
    } catch (error) {
        console.warn(`Failed to fetch ${handler.url}:`, error);
        return handler.defaultValue;
    }
}

export async function fetchStats(): Promise<StatsData> {
    const [totalEntries, lastUpdateTime, isHealthy] = await Promise.all([
        fetchEndpoint(apiHandlers.totalEntries),
        fetchEndpoint(apiHandlers.lastUpdateTime),
        fetchEndpoint(apiHandlers.isHealthy),
    ]);

    return {
        totalEntries,
        lastUpdateTime,
        isHealthy,
    };
}
