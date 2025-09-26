import React from 'react';
import { ApiResponse } from '../types/api';

interface PaginationControlsProps {
  apiResponse: ApiResponse;
  isLoading: boolean;
  onPreviousPage: () => void;
  onNextPage: () => void;
  currentPage?: number;
  variant?: 'inline' | 'footer';
}

const PaginationControls: React.FC<PaginationControlsProps> = ({
  apiResponse,
  isLoading,
  onPreviousPage,
  onNextPage,
  currentPage,
  variant = 'inline'
}) => {
  const previousText = variant === 'footer' ? '← Previous Page' : '← Previous';
  const nextText = variant === 'footer' ? 'Next Page →' : 'Next →';

  return (
    <div className="pagination-controls">
      <button
        type="button"
        onClick={onPreviousPage}
        disabled={!apiResponse.has_previous || isLoading}
        className="pagination-button"
      >
        {previousText}
      </button>
      {variant === 'footer' && currentPage && (
        <span className="pagination-info">
          Page {currentPage}
        </span>
      )}
      <button
        type="button"
        onClick={onNextPage}
        disabled={!apiResponse.has_next || isLoading}
        className="pagination-button"
      >
        {nextText}
      </button>
    </div>
  );
};

export default PaginationControls;
