import axios from 'axios';

// Resolve API base URL with sensible defaults:
// 1) Use VITE_API_URL when provided.
// 2) Otherwise, use same-origin relative paths (empty baseURL) so a dev proxy can handle /api.
// 3) Avoid hard-coding an unknown host like "host" which can fail DNS resolution.
const envBase = (import.meta as any).env?.VITE_API_URL as string | undefined;
const API_BASE_URL = envBase && envBase.trim().length > 0
    ? envBase.replace(/\/+$/g, '')
    : '';

const apiClient = axios.create({
    baseURL: API_BASE_URL,
    headers: {
        'Content-Type': 'application/json',
    },
});

export type LocalSource = {
    type: 'local';
    root: string;
    max_rows?: number;
};

export type SQLSource = {
    type: 'sql';
    query?: string;
    schema?: string;
    max_rows?: number;
};

export interface RunRequest {
    source: LocalSource | SQLSource;
    mapping: {
        reference_fields: string[];
        synonyms?: Record<string, string[]>;
        threshold?: number;
        epsilon?: number;
    };
    anomaly?: {
        z_threshold?: number;
        use_iqr?: boolean;
        use_zscore?: boolean;
        use_isolation_forest?: boolean;
        use_missing_values?: boolean;
        missing_threshold?: number;
        contamination?: number;
        n_estimators?: number;
        random_state?: number;
    };
}

export interface DatasetReport {
    name: string;
    path: string | null;
    rows: number;
    cols: number;
    schema: Record<string, string>;
    semantic_types: Record<string, string>;
    statistics: {
        missing_ratios: Record<string, number>;
        numeric_stats?: Record<string, { mean: number; min: number; max: number; std: number; zeros: number }>;
        text_stats?: Record<string, { unique_count: number; top_values: Array<{ value: string; count: number }> }>;
        row_count: number;
        col_count: number;
    };
    nested_structures: string[];
    categorical_cols: string[];
    llm_insights: {
        descriptions: Record<string, string>;
        summary: string | null;
        anomaly_explanation: string | null;
    };
    mapping: Record<string, any>;
    ambiguous: string[];
    unmapped: string[];
    anomalies: Record<string, number>;
    anomaly_samples_saved: Record<string, string | null>;
    anomaly_rows?: Record<string, number[]>;
    anomaly_previews?: Record<string, Array<Record<string, any>>>;
}

export interface AssistantReport {
    data_root: string | null;
    datasets: DatasetReport[];
}

export const api = {
    health: async () => {
        const response = await apiClient.get('/api/v1/health');
        return response.data;
    },

    runAssistant: async (request: RunRequest, mode: 'sync' | 'async' = 'sync'): Promise<AssistantReport> => {
        const response = await apiClient.post(`/api/v1/run?mode=${mode}`, request);
        return response.data;
    },

    runAssistantStream: async (
        request: RunRequest,
        onProgress: (msg: string, pct: number) => void
    ): Promise<AssistantReport> => {
        const response = await fetch(`${API_BASE_URL}/api/v1/run_stream`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(request),
        });

        if (!response.ok) {
            const errText = await response.text();
            throw new Error(`HTTP Error ${response.status}: ${errText}`);
        }

        if (!response.body) throw new Error('No response body available from server');

        const reader = response.body.getReader();
        const decoder = new TextDecoder('utf-8');
        let buffer = '';

        while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            buffer += decoder.decode(value, { stream: true });

            // SSE messages are separated by \n\n
            const chunks = buffer.split('\n\n');
            // Keep the last incomplete chunk in the buffer
            buffer = chunks.pop() || '';

            for (const chunk of chunks) {
                const lines = chunk.split('\n');
                for (const line of lines) {
                    if (line.startsWith('data: ')) {
                        const dataStr = line.substring(6).trim();
                        if (!dataStr) continue;
                        try {
                            const event = JSON.parse(dataStr);
                            if (event.type === 'progress') {
                                onProgress(event.message, event.percent);
                            } else if (event.type === 'complete') {
                                return event.report;
                            } else if (event.type === 'error') {
                                throw new Error(event.message);
                            }
                        } catch (e) {
                            console.error('Failed to parse SSE event:', e, 'Data:', dataStr);
                        }
                    }
                }
            }
        }
        throw new Error('Stream ended unexpectedly without completion');
    },

    getReport: async (reportId: string): Promise<any> => {
        const response = await apiClient.get(`/api/v1/reports/${reportId}`);
        return response.data;
    },

    listArtifacts: async (reportId: string) => {
        const response = await apiClient.get(`/api/v1/reports/${reportId}/artifacts`);
        return response.data;
    },

    listTables: async (schema: string = 'lakehouse.datalake.raw') => {
        const response = await apiClient.get(`/api/v1/tables`, { params: { schema } });
        return response.data;
    },

    listSchemas: async () => {
        const response = await apiClient.get('/api/v1/schemas');
        return response.data;
    },
};

export default api;
