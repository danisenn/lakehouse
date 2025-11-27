import { useState, useEffect } from 'react';
import api, { type RunRequest } from '../services/api';
import Logger from '../services/logger';

export default function ConfigPanel({ onRunComplete }: { onRunComplete: (report: any) => void }) {
    const [mode, setMode] = useState<'local' | 'single_table' | 'all_tables' | 'custom_query'>('local');
    const [loading, setLoading] = useState(false);
    const [refFields, setRefFields] = useState('label,title,text');
    const [threshold, setThreshold] = useState(0.7);

    // State for SQL modes
    const [sqlSchema, setSqlSchema] = useState('lakehouse.datalake.raw');
    const [sqlTable, setSqlTable] = useState('SF_incidents2016.json');
    const [customQuery, setCustomQuery] = useState('SELECT * FROM "Samples"."samples.dremio.com"."SF_incidents2016.json" LIMIT 10');
    const [availableTables, setAvailableTables] = useState<string[]>([]);

    // Fetch tables when schema changes
    useEffect(() => {
        if (mode === 'single_table') {
            api.listTables(sqlSchema)
                .then(data => {
                    setAvailableTables(data.tables);
                    if (data.tables.length > 0 && !data.tables.includes(sqlTable)) {
                        setSqlTable(data.tables[0]);
                    }
                })
                .catch(err => Logger.error('Failed to fetch tables:', err));
        }
    }, [mode, sqlSchema]);

    // Construct the request based on the selected mode
    let source: RunRequest['source'];
    if (mode === 'local') {
        source = {
            type: 'local',
            root: 'data',
            max_rows: 100,
        };
    } else if (mode === 'single_table') {
        source = {
            type: 'sql',
            query: `SELECT * FROM "${sqlSchema}"."${sqlTable}" LIMIT 100`,
            max_rows: 100,
        };
    } else if (mode === 'all_tables') {
        source = {
            type: 'sql',
            schema: sqlSchema,
            max_rows: 100,
        };
    } else {
        // custom_query
        source = {
            type: 'sql',
            query: customQuery,
            max_rows: 100,
        };
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

    const handleRun = async () => {
        setLoading(true);
        Logger.info('Starting assistant run...', request);
        try {
            const report = await api.runAssistant(request, 'sync');
            Logger.info('Run completed successfully', report);
            onRunComplete(report);
        } catch (error) {
            Logger.error('Run failed:', error);
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
                        <option value="single_table">SQL: Single Table</option>
                        <option value="all_tables">SQL: All Tables in Schema</option>
                        <option value="custom_query">SQL: Custom Query</option>
                    </select>
                </div>

                {/* Schema Input - Shared by Single Table and All Tables */}
                {(mode === 'single_table' || mode === 'all_tables') && (
                    <div>
                        <label className="block text-sm font-medium mb-2">Schema Name</label>
                        <input
                            type="text"
                            value={sqlSchema}
                            onChange={(e) => setSqlSchema(e.target.value)}
                            className="w-full p-2 bg-gray-700 rounded"
                            placeholder='lakehouse.datalake.raw'
                        />
                    </div>
                )}

                {/* Table Input - Only for Single Table */}
                {mode === 'single_table' && (
                    <div>
                        <label className="block text-sm font-medium mb-2">Table Name</label>
                        {availableTables.length > 0 ? (
                            <select
                                value={sqlTable}
                                onChange={(e) => setSqlTable(e.target.value)}
                                className="w-full p-2 bg-gray-700 rounded"
                            >
                                {availableTables.map(t => (
                                    <option key={t} value={t}>{t}</option>
                                ))}
                            </select>
                        ) : (
                            <input
                                type="text"
                                value={sqlTable}
                                onChange={(e) => setSqlTable(e.target.value)}
                                className="w-full p-2 bg-gray-700 rounded"
                                placeholder="SF_incidents2016.json"
                            />
                        )}
                    </div>
                )}

                {/* Custom Query Input */}
                {mode === 'custom_query' && (
                    <div>
                        <label className="block text-sm font-medium mb-2">SQL Query</label>
                        <textarea
                            value={customQuery}
                            onChange={(e) => setCustomQuery(e.target.value)}
                            className="w-full p-2 bg-gray-700 rounded h-24 font-mono text-sm"
                            placeholder="SELECT * FROM table LIMIT 10"
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

                <div className="mt-4">
                    <label className="block text-sm font-medium mb-2">Request Preview</label>
                    <pre className="bg-gray-900 p-4 rounded overflow-auto text-xs font-mono text-green-400 max-h-64">
                        {JSON.stringify(request, null, 2)}
                    </pre>
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
