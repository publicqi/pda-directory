import React, { KeyboardEventHandler, useLayoutEffect, useRef, useState } from 'react';
import useCopyToClipboard from '../../hooks/useCopyToClipboard';

interface TogglableSeedValueProps {
    valueA: string;
    valueB: string;
    label: string;
    badgeClass?: string;
}

type Presentation = 'A' | 'B';

const TogglableSeedValue = ({ valueA, valueB, label, badgeClass = '' }: TogglableSeedValueProps) => {
    const [presentation, setPresentation] = useState<Presentation>('A');
    const displayValue = presentation === 'A' ? valueA : valueB;
    const { copy, copied } = useCopyToClipboard(displayValue);
    const [isHovered, setIsHovered] = useState(false);
    const refA = useRef<HTMLSpanElement>(null);
    const refB = useRef<HTMLSpanElement>(null);
    const [faderWidth, setFaderWidth] = useState<number | 'auto'>('auto');

    useLayoutEffect(() => {
        let newWidth = 0;
        if (presentation === 'A' && refA.current) {
            newWidth = refA.current.offsetWidth;
        } else if (presentation === 'B' && refB.current) {
            newWidth = refB.current.offsetWidth;
        }
        if (newWidth > 0) {
            setFaderWidth(newWidth);
        }
    }, [presentation, valueA, valueB]);

    const handleToggle = () => {
        setPresentation((prev) => (prev === 'A' ? 'B' : 'A'));
    };

    const handleKeyDown: KeyboardEventHandler<HTMLDivElement> = (event) => {
        if (event.key === 'Enter' || event.key === ' ') {
            event.preventDefault();
            handleToggle();
        }
    };

    const handleCopyClick = (e: React.MouseEvent) => {
        e.preventDefault();
        e.stopPropagation();
        copy();
    };

    return (
        <div className="seed-value-layout">
            <div className="seed-tag">
                <span className={`badge ${badgeClass}`}>{label}</span>
            </div>
            <div
                className={`seed-interactive-container ${isHovered ? 'hovered' : ''}`}
                onMouseEnter={() => setIsHovered(true)}
                onMouseLeave={() => setIsHovered(false)}
            >
                <div
                    onClick={handleToggle}
                    onKeyDown={handleKeyDown}
                    role="button"
                    tabIndex={0}
                    className="seed-value-interactive-area"
                >
                    <div className="value-mono value-fader" style={{ width: faderWidth }}>
                        <span ref={refA} className={presentation === 'A' ? 'visible' : 'hidden'}>
                            {valueA}
                        </span>
                        <span ref={refB} className={presentation === 'B' ? 'visible' : 'hidden'}>
                            {valueB}
                        </span>
                    </div>
                </div>
                <div className="seed-actions">
                    <button
                        className="seed-action-btn"
                        onClick={handleCopyClick}
                        title="Copy value"
                        aria-label="Copy value"
                        disabled={copied}
                    >
                        {copied ? (
                            <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor">
                                <path d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z" />
                            </svg>
                        ) : (
                            <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor">
                                <path d="M16 1H4c-1.1 0-2 .9-2 2v14h2V3h12V1zm3 4H8c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h11c1.1 0 2-.9 2-2V7c0-1.1-.9-2-2-2zm0 16H8V7h11v14z" />
                            </svg>
                        )}
                    </button>
                </div>
            </div>
        </div>
    );
};

export default TogglableSeedValue;
