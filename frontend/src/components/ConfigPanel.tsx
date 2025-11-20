import { useState } from 'react';
import api, { type RunRequest, type LocalSource, type SQLSource } from '../services/api';

export default function ConfigPanel({ onRunComplete }: { onRunComplete: (report: any) => void }) {
    const [mode, setMode] = useState<'local' | 'query' | 'schema'>('local');
    const [loading, setLoading] = useState(false);
    const [refFields, setRefFields] = useState('label,title,text');
    const [threshold, setThreshold] = useState(0.7);
    const [sqlQuery, setSqlQuery] = useState('SELECT * FROM "Samples"."samples.dremio.com"."SF_incidents2016.json" LIMIT 10');
    const [sqlSchema, setSqlSchema] = useState('Samples."samples.dremio.com"');

    const handleRun = async () => {
        setLoading(true);
        try {
            let source: RunRequest['source'];
            if (mode === 'local') {
                const localSource: LocalSource = {
                    type: 'local',
                    root: 'data',
                    max_rows: 100,
                };
                source = localSource;
            } else {
                const sqlSource: SQLSource = {
                    type: 'sql',
                    query: mode === 'query' ? sqlQuery : undefined,
                    schema: mode === 'schema' ? sqlSchema : undefined,
                    max_rows: 100,
                };
                source = sqlSource;
            }

            const request: RunRequest = {
                source,
                mapping: {
                    reference_fields: refFields.split(',').map(f => f.trim()),
                    threshold,
                    epsilon: 0.05,
                },
                anomaly: {
                    use_zscore: true,
                    use_iqr: true,
                    use_isolation_forest: true,
                },
            };

            const report = await api.runAssistant(request, 'sync');
            onRunComplete(report);
        } catch (error) {
            console.error('Run failed:', error);
            alert('Run failed. See console for details.');
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="bg-gray-800 p-6 rounded-lg shadow-lg">
            <h2 className="text-2xl font-bold mb-4">Configuration</h2>

            <div className="space-y-4">
                <div>
                    <label className="block text-sm font-medium mb-2">Mode</label>
                    <select
                        value={mode}
                        onChange={(e) => setMode(e.target.value as any)}
                        className="w-full p-2 bg-gray-700 rounded"
                    >
                        <option value="local">Local Files</option>
                        <option value="query">SQL Query</option>
                        <option value="schema">Schema Discovery</option>
                    </select>
                </div>

                {mode === 'query' && (
                    <div>
                        <label className="block text-sm font-medium mb-2">SQL Query</label>
                        <textarea
                            value={sqlQuery}
                            onChange={(e) => setSqlQuery(e.target.value)}
                            className="w-full p-2 bg-gray-700 rounded h-24 font-mono text-sm"
                            placeholder="SELECT * FROM table LIMIT 10"
                        />
                    </div>
                )}

                {mode === 'schema' && (
                    <div>
                        <label className="block text-sm font-medium mb-2">Schema Name</label>
                        <input
                            type="text"
                            value={sqlSchema}
                            onChange={(e) => setSqlSchema(e.target.value)}
                            className="w-full p-2 bg-gray-700 rounded"
                            placeholder="Source.Schema"
                        />
                    </div>
                )}

                <div>
                    <label className="block text-sm font-medium mb-2">Reference Fields</label>
                    <input
                        type="text"
                        value={refFields}
                        onChange={(e) => setRefFields(e.target.value)}
                        placeholder="label,title,text"
                        className="w-full p-2 bg-gray-700 rounded"
                    />
                </div>

                <div>
                    <label className="block text-sm font-medium mb-2">Threshold: {threshold}</label>
                    <input
                        type="range"
                        min="0"
                        max="1"
                        step="0.01"
                        value={threshold}
                        onChange={(e) => setThreshold(parseFloat(e.target.value))}
                        className="w-full"
                    />
                </div>

                <button
                    onClick={handleRun}
                    disabled={loading}
                    className="w-full bg-blue-600 hover:bg-blue-700 disabled:bg-gray-600 py-2 px-4 rounded font-semibold"
                >
                    {loading ? 'Running...' : 'Run Assistant'}
                </button>
            </div>
        </div>
    );
}
