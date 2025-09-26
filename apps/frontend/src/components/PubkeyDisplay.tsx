import { useState } from 'react';

interface PubkeyDisplayProps {
  pubkey: string;
  className?: string;
}

const PubkeyDisplay = ({ pubkey, className = 'value-mono' }: PubkeyDisplayProps) => {
  const [isHovered, setIsHovered] = useState(false);

  const handleCopy = async (e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    try {
      await navigator.clipboard.writeText(pubkey);
    } catch (err) {
      console.error('Failed to copy pubkey:', err);
    }
  };

  const handleSolscanClick = (e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    window.open(`https://solscan.io/address/${pubkey}`, '_blank', 'noopener,noreferrer');
  };

  return (
    <div
      className={`pubkey-display ${className} ${isHovered ? 'pubkey-hovered' : ''}`}
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
    >
      <span className="pubkey-text">{pubkey}</span>
      <div className="pubkey-actions">
        <button
          className="pubkey-action-btn"
          onClick={handleCopy}
          title="Copy address"
          aria-label="Copy address"
        >
          <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor">
            <path d="M16 1H4c-1.1 0-2 .9-2 2v14h2V3h12V1zm3 4H8c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h11c1.1 0 2-.9 2-2V7c0-1.1-.9-2-2-2zm0 16H8V7h11v14z" />
          </svg>
        </button>
        <button
          className="pubkey-action-btn"
          onClick={handleSolscanClick}
          title="View on Solscan"
          aria-label="View on Solscan"
        >
          <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor">
            <path d="M19 19H5V5h7V3H5c-1.11 0-2 .9-2 2v14c0 1.1.89 2 2 2h14c1.1 0 2-.9 2-2v-7h-2v7zM14 3v2h3.59l-9.83 9.83 1.41 1.41L19 6.41V10h2V3h-7z" />
          </svg>
        </button>
      </div>
    </div>
  );
};

export default PubkeyDisplay;
