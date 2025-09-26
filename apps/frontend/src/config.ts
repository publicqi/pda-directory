const apiBase = import.meta.env.API_BASE_URL ?? 'http://localhost:8000';
const baseUrl = import.meta.env.BASE_URL ?? 'http://localhost:5173';

export const API_BASE_URL = apiBase.replace(/\/$/, '');
export const BASE_URL = baseUrl.replace(/\/$/, '');