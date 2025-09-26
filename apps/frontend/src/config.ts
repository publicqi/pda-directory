const apiBase = import.meta.env.VITE_API_BASE_URL ?? 'https://api.pda.directory';
const baseUrl = import.meta.env.VITE_BASE_URL ?? 'http://localhost:5173';

export const API_BASE_URL = apiBase.replace(/\/$/, '');
export const BASE_URL = baseUrl.replace(/\/$/, '');