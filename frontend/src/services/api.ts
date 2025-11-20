import axios from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://host:8000';

const apiClient = axios.create({
    baseURL: API_BASE_URL,
    headers: {
        'Content-Type': 'application/json',
    },
});

export interface RunRequest {
    source: {
        root?: string;
        max_rows?: number;
    };
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
    mapping: Record<string, any>;
    ambiguous: string[];
    unmapped: string[];
    anomalies: Record<string, number>;
    anomaly_samples_saved: Record<string, string | null>;
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

    getReport: async (reportId: string): Promise<any> => {
        const response = await apiClient.get(`/api/v1/reports/${reportId}`);
        return response.data;
    },

    listArtifacts: async (reportId: string) => {
        const response = await apiClient.get(`/api/v1/reports/${reportId}/artifacts`);
        return response.data;
    },
};

export default api;
