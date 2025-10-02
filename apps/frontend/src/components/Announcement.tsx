import React, { useEffect, useState } from 'react';
import { API_BASE_URL } from '../config';

interface AnnouncementData {
    message: string;
    enabled: boolean;
}

const Announcement: React.FC = () => {
    const [announcement, setAnnouncement] = useState<AnnouncementData | null>(null);
    const [isVisible, setIsVisible] = useState(false);

    useEffect(() => {
        const fetchAnnouncement = async () => {
            try {
                const response = await fetch(`${API_BASE_URL}/api/announcement`);
                if (!response.ok) {
                    return;
                }
                const data: AnnouncementData = await response.json();
                if (data.enabled && data.message) {
                    // Check if user has already dismissed this specific announcement
                    const dismissedKey = `announcement-dismissed-${btoa(data.message)}`;
                    const isDismissed = localStorage.getItem(dismissedKey);
                    if (!isDismissed) {
                        setAnnouncement(data);
                        setIsVisible(true);
                    }
                }
            } catch (error) {
                console.error('Failed to fetch announcement:', error);
            }
        };

        fetchAnnouncement();
    }, []);

    const handleDismiss = () => {
        if (announcement) {
            const dismissedKey = `announcement-dismissed-${btoa(announcement.message)}`;
            localStorage.setItem(dismissedKey, 'true');
        }
        setIsVisible(false);
    };

    if (!isVisible || !announcement) {
        return null;
    }

    return (
        <>
            <div className="announcement-overlay" onClick={handleDismiss} />
            <div className="announcement-popup">
                <div className="announcement-header">
                    <h3>Announcement</h3>
                    <button className="announcement-close" onClick={handleDismiss} aria-label="Close announcement">
                        ×
                    </button>
                </div>
                <div className="announcement-content">
                    <p>{announcement.message}</p>
                </div>
                <div className="announcement-footer">
                    <button className="announcement-button" onClick={handleDismiss}>
                        Got it
                    </button>
                </div>
            </div>
        </>
    );
};

export default Announcement;


