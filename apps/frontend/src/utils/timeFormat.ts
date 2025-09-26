/**
 * Format a timestamp as "x mins y hrs z days ago"
 * Only shows non-zero parts (e.g., "5 mins ago", "2 hrs 30 mins ago", "1 day 3 hrs ago")
 */
export function formatTimeAgo(timestamp: number): string {
    const now = Date.now();
    const diffMs = now - timestamp * 1000; // Convert Unix timestamp to milliseconds

    if (diffMs < 0) {
        return 'in the future';
    }

    const diffSeconds = Math.floor(diffMs / 1000);
    const diffMinutes = Math.floor(diffSeconds / 60);
    const diffHours = Math.floor(diffMinutes / 60);
    const diffDays = Math.floor(diffHours / 24);

    const parts: string[] = [];

    if (diffDays > 0) {
        parts.push(`${diffDays} day${diffDays === 1 ? '' : 's'}`);
    }

    const remainingHours = diffHours % 24;
    if (remainingHours > 0) {
        parts.push(`${remainingHours} hr${remainingHours === 1 ? '' : 's'}`);
    }

    const remainingMinutes = diffMinutes % 60;
    if (remainingMinutes > 0) {
        parts.push(`${remainingMinutes} min${remainingMinutes === 1 ? '' : 's'}`);
    }

    if (parts.length === 0) {
        return 'just now';
    }

    return `${parts.join(' ')} ago`;
}
