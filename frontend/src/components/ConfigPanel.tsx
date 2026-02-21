import { useState, useEffect } from 'react';
import { Settings, Database, FileText, Play, Terminal, Layers, AlertCircle } from 'lucide-react';
import toast from 'react-hot-toast';
import api, { type RunRequest } from '../services/api';
import Logger from '../services/logger';
import { Card, CardContent, CardDescription, CardHeader, CardTitle, CardFooter } from './ui/Card';
import { Button } from './ui/Button';
import { Input } from './ui/Input';
import { Select } from './ui/Select';
import { Badge } from './ui/Badge';
import { InfoTooltip } from './ui/InfoTooltip';

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
            const toastId = toast.loading('Fetching tables...', { id: 'fetch-tables' });
            api.listTables(sqlSchema)
                .then(data => {
                    setAvailableTables(data.tables);
                    if (data.tables.length > 0 && !data.tables.includes(sqlTable)) {
                        setSqlTable(data.tables[0]);
                    }
                    toast.success('Tables loaded', { id: toastId });
                })
                .catch(err => {
                    Logger.error('Failed to fetch tables:', err);
                    toast.error('Failed to fetch tables', { id: toastId });
                });
        }
    }, [mode, sqlSchema]);

    // Construct the request based on the selected mode
    let source: RunRequest['source'];
    if (mode === 'local') {
        source = {
            type: 'local',
            root: 'data',
            max_rows: undefined,
        };
    } else if (mode === 'single_table') {
        source = {
            type: 'sql',
            query: `SELECT * FROM ${sqlSchema}."${sqlTable}" LIMIT 100`,
            max_rows: undefined,
        };
    } else if (mode === 'all_tables') {
        source = {
            type: 'sql',
            schema: sqlSchema,
            max_rows: undefined,
        };
    } else {
        // custom_query
        source = {
            type: 'sql',
            query: customQuery,
            max_rows: undefined,
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
            use_missing_values: true,
            missing_threshold: 1,
        },
    };

    const handleRun = async () => {
        setLoading(true);
        const toastId = toast.loading('Running assistant...');
        Logger.info('Starting assistant run...', request);
        try {
            const report = await api.runAssistant(request, 'sync');
            Logger.info('Run completed successfully', report);
            onRunComplete(report);
            toast.success('Run completed successfully!', { id: toastId });
        } catch (error) {
            Logger.error('Run failed:', error);
            toast.error('Run failed. Check console for details.', { id: toastId });
        } finally {
            setLoading(false);
        }
    };

    return (
        <Card className="h-full border-gray-800 bg-gray-900/50 backdrop-blur-sm">
            <CardHeader>
                <CardTitle className="flex items-center gap-2">
                    <Settings className="h-5 w-5 text-blue-500" />
                    Configuration
                </CardTitle>
                <CardDescription>
                    Configure the data source and analysis parameters.
                </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
                <div className="space-y-2">
                    <label className="text-sm font-medium text-gray-300 flex items-center gap-2">
                        <Database className="h-4 w-4 text-gray-500" />
                        Source Mode
                        <InfoTooltip content="Choose where the data comes from: local options or SQL queries." />
                    </label>
                    <Select
                        value={mode}
                        onChange={(e) => setMode(e.target.value as any)}
                    >
                        <option value="local">Local Files</option>
                        <option value="single_table">SQL: Single Table</option>
                        <option value="all_tables">SQL: All Tables in Schema</option>
                        <option value="custom_query">SQL: Custom Query</option>
                    </Select>
                </div>

                {/* Schema Input - Shared by Single Table and All Tables */}
                {(mode === 'single_table' || mode === 'all_tables') && (
                    <div className="space-y-2">
                        <label className="text-sm font-medium text-gray-300 flex items-center gap-2">
                            <Layers className="h-4 w-4 text-gray-500" />
                            Schema Name
                        </label>
                        <Input
                            type="text"
                            value={sqlSchema}
                            onChange={(e) => setSqlSchema(e.target.value)}
                            placeholder='lakehouse.datalake.raw'
                        />
                    </div>
                )}

                {/* Table Input - Only for Single Table */}
                {mode === 'single_table' && (
                    <div className="space-y-2">
                        <label className="text-sm font-medium text-gray-300 flex items-center gap-2">
                            <FileText className="h-4 w-4 text-gray-500" />
                            Table Name
                        </label>
                        {availableTables.length > 0 ? (
                            <Select
                                value={sqlTable}
                                onChange={(e) => setSqlTable(e.target.value)}
                            >
                                {availableTables.map(t => (
                                    <option key={t} value={t}>{t}</option>
                                ))}
                            </Select>
                        ) : (
                            <Input
                                type="text"
                                value={sqlTable}
                                onChange={(e) => setSqlTable(e.target.value)}
                                placeholder="SF_incidents2016.json"
                            />
                        )}
                    </div>
                )}

                {/* Custom Query Input */}
                {mode === 'custom_query' && (
                    <div className="space-y-2">
                        <label className="text-sm font-medium text-gray-300 flex items-center gap-2">
                            <Terminal className="h-4 w-4 text-gray-500" />
                            SQL Query
                        </label>
                        <textarea
                            value={customQuery}
                            onChange={(e) => setCustomQuery(e.target.value)}
                            className="w-full p-3 bg-gray-950 border border-gray-800 rounded-md h-32 font-mono text-xs text-blue-300 focus:outline-none focus:ring-2 focus:ring-blue-600 focus:border-transparent resize-none"
                            placeholder="SELECT * FROM table LIMIT 10"
                        />
                    </div>
                )}

                <div className="space-y-2">
                    <label className="text-sm font-medium text-gray-300 flex items-center gap-2">
                        <AlertCircle className="h-4 w-4 text-gray-500" />
                        Reference Fields
                        <InfoTooltip content="Comma-separated list of field names (e.g., label, title) used to align metrics across datasets." />
                    </label>
                    <Input
                        type="text"
                        value={refFields}
                        onChange={(e) => setRefFields(e.target.value)}
                        placeholder="label,title,text"
                    />
                    <p className="text-xs text-gray-500">Comma-separated list of fields to map against.</p>
                </div>

                <div className="space-y-4 pt-2">
                    <div className="flex justify-between items-center">
                        <label className="text-sm font-medium text-gray-300 flex items-center gap-2">
                            Similarity Threshold
                            <InfoTooltip content="Minimum score (0-1) required for two fields to be considered a match." />
                        </label>
                        <Badge variant="outline" className="font-mono">{threshold.toFixed(2)}</Badge>
                    </div>
                    <input
                        type="range"
                        min="0"
                        max="1"
                        step="0.01"
                        value={threshold}
                        onChange={(e) => setThreshold(parseFloat(e.target.value))}
                        className="w-full h-2 bg-gray-800 rounded-lg appearance-none cursor-pointer accent-blue-600"
                    />
                </div>
            </CardContent>
            <CardFooter className="flex-col gap-4">
                <Button
                    onClick={handleRun}
                    disabled={loading}
                    className="w-full h-12 text-lg shadow-blue-900/20 shadow-lg"
                >
                    {loading ? (
                        <>
                            <div className="h-5 w-5 border-2 border-white/30 border-t-white rounded-full animate-spin mr-2" />
                            Running Analysis...
                        </>
                    ) : (
                        <>
                            <Play className="h-5 w-5 mr-2 fill-current" />
                            Run Assistant
                        </>
                    )}
                </Button>

                <div className="w-full">
                    <details className="text-xs text-gray-500 cursor-pointer group">
                        <summary className="hover:text-gray-400 transition-colors list-none flex items-center gap-1">
                            <span className="group-open:rotate-90 transition-transform">▶</span>
                            Debug Request Payload
                        </summary>
                        <pre className="mt-2 p-3 bg-gray-950 rounded border border-gray-800 overflow-auto font-mono text-xs text-green-500 max-h-40">
                            {JSON.stringify(request, null, 2)}
                        </pre>
                    </details>
                </div>
            </CardFooter>
        </Card>
    );
}
