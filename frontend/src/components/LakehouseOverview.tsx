import { useState, useEffect } from 'react';
import { X, Database, Table, ChevronRight, ChevronDown, Search, Loader2 } from 'lucide-react';
import api from '../services/api';
import Logger from '../services/logger';
import { Card, CardHeader, CardTitle, CardContent } from './ui/Card';
import { Button } from './ui/Button';
import { Input } from './ui/Input';
import { Badge } from './ui/Badge';

interface LakehouseOverviewProps {
    isOpen: boolean;
    onClose: () => void;
}

interface SchemaData {
    name: string;
    tables: string[] | null; // null means not loaded yet
    loading: boolean;
    error: string | null;
}

export default function LakehouseOverview({ isOpen, onClose }: LakehouseOverviewProps) {
    const [schemas, setSchemas] = useState<Record<string, SchemaData>>({});
    const [loadingSchemas, setLoadingSchemas] = useState(false);
    const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(new Set());
    const [searchTerm, setSearchTerm] = useState('');

    useEffect(() => {
        if (isOpen) {
            loadSchemas();
            // Lock body scroll
            document.body.style.overflow = 'hidden';
        } else {
            document.body.style.overflow = 'unset';
            // Reset state on close
            setSearchTerm('');
            setExpandedSchemas(new Set());
        }
        return () => {
            document.body.style.overflow = 'unset';
        };
    }, [isOpen]);

    const loadSchemas = async () => {
        setLoadingSchemas(true);
        try {
            const data = await api.listSchemas();
            // Use type assertion if necessary, assuming API returns { schemas: string[] }
            const schemaList = (data as any).schemas || [];

            const initialMap: Record<string, SchemaData> = {};
            schemaList.forEach((s: string) => {
                initialMap[s] = {
                    name: s,
                    tables: null,
                    loading: false,
                    error: null
                };
            });
            setSchemas(initialMap);
        } catch (err) {
            Logger.error('Failed to load schemas', err);
        } finally {
            setLoadingSchemas(false);
        }
    };

    const toggleSchema = async (schemaName: string) => {
        const newExpanded = new Set(expandedSchemas);
        const isExpanding = !newExpanded.has(schemaName);

        if (isExpanding) {
            newExpanded.add(schemaName);

            // Load tables if not already loaded
            if (schemas[schemaName].tables === null) {
                setSchemas(prev => ({
                    ...prev,
                    [schemaName]: { ...prev[schemaName], loading: true, error: null }
                }));

                try {
                    const data = await api.listTables(schemaName);
                    setSchemas(prev => ({
                        ...prev,
                        [schemaName]: {
                            ...prev[schemaName],
                            tables: data.tables || [],
                            loading: false
                        }
                    }));
                } catch (err) {
                    Logger.error(`Failed to load tables for ${schemaName}`, err);
                    setSchemas(prev => ({
                        ...prev,
                        [schemaName]: {
                            ...prev[schemaName],
                            loading: false,
                            error: 'Failed to load tables'
                        }
                    }));
                }
            }
        } else {
            newExpanded.delete(schemaName);
        }
        setExpandedSchemas(newExpanded);
    };

    if (!isOpen) return null;

    const filteredSchemaNames = Object.keys(schemas)
        .filter(name => name.toLowerCase().includes(searchTerm.toLowerCase()))
        .sort();

    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm p-4">
            <Card className="w-full max-w-4xl max-h-[85vh] flex flex-col bg-gray-900 border-gray-800 shadow-2xl">
                <CardHeader className="flex flex-row items-center justify-between border-b border-gray-800 bg-gray-900/50">
                    <div>
                        <CardTitle className="flex items-center gap-2 text-xl">
                            <Database className="h-6 w-6 text-blue-500" />
                            Lakehouse Explorer
                        </CardTitle>
                        <p className="text-sm text-gray-500 mt-1">
                            Browse available schemas and tables in the data lake.
                        </p>
                    </div>
                    <Button
                        variant="ghost"
                        size="icon"
                        onClick={onClose}
                        className="text-gray-400 hover:text-white"
                    >
                        <X className="h-6 w-6" />
                    </Button>
                </CardHeader>

                <div className="p-4 border-b border-gray-800 bg-gray-900/30">
                    <div className="relative">
                        <Search className="absolute left-3 top-3 h-4 w-4 text-gray-500" />
                        <Input
                            value={searchTerm}
                            onChange={(e) => setSearchTerm(e.target.value)}
                            placeholder="Filter schemas..."
                            className="pl-10 bg-gray-950 border-gray-800"
                        />
                    </div>
                </div>

                <CardContent className="flex-1 overflow-y-auto p-4 custom-scrollbar">
                    {loadingSchemas ? (
                        <div className="flex flex-col items-center justify-center p-12 text-gray-500">
                            <Loader2 className="h-8 w-8 animate-spin mb-4 text-blue-500" />
                            <p>Discovering schemas...</p>
                        </div>
                    ) : filteredSchemaNames.length === 0 ? (
                        <div className="text-center p-12 text-gray-500 border border-dashed border-gray-800 rounded-lg">
                            <Database className="h-12 w-12 mx-auto mb-4 opacity-20" />
                            <p>No schemas found matching your filter.</p>
                        </div>
                    ) : (
                        <div className="space-y-2">
                            {filteredSchemaNames.map(schemaName => {
                                const schema = schemas[schemaName];
                                const isExpanded = expandedSchemas.has(schemaName);
                                const tableCount = schema.tables?.length ?? '?';

                                return (
                                    <div key={schemaName} className="border border-gray-800 rounded-lg overflow-hidden bg-gray-950/30">
                                        <div
                                            className={`
                                                flex items-center justify-between p-3 cursor-pointer transition-colors
                                                ${isExpanded ? 'bg-blue-900/20' : 'hover:bg-gray-900'}
                                            `}
                                            onClick={() => toggleSchema(schemaName)}
                                        >
                                            <div className="flex items-center gap-3">
                                                {isExpanded ? (
                                                    <ChevronDown className="h-4 w-4 text-blue-500" />
                                                ) : (
                                                    <ChevronRight className="h-4 w-4 text-gray-600" />
                                                )}
                                                <div className="flex flex-col">
                                                    <span className="font-medium text-gray-200">{schemaName}</span>
                                                </div>
                                            </div>
                                            <div className="flex items-center gap-2">
                                                {tableCount !== '?' && (
                                                    <Badge variant="outline" className="text-xs bg-gray-900 border-gray-700 text-gray-400">
                                                        {tableCount} Tables
                                                    </Badge>
                                                )}
                                                {schema.loading && <Loader2 className="h-3 w-3 animate-spin text-blue-400" />}
                                                <Badge variant="outline" className="text-xs bg-gray-900 border-gray-700 text-gray-400">
                                                    SCHEMA
                                                </Badge>
                                            </div>
                                        </div>

                                        {isExpanded && (
                                            <div className="border-t border-gray-800 bg-gray-950 p-2 pl-10 animate-in slide-in-from-top-2 duration-200">
                                                {schema.loading && !schema.tables ? (
                                                    <div className="py-2 text-sm text-gray-500 flex items-center gap-2">
                                                        <Loader2 className="h-3 w-3 animate-spin" />
                                                        Loading tables...
                                                    </div>
                                                ) : schema.error ? (
                                                    <div className="py-2 text-sm text-red-400">
                                                        {schema.error}
                                                    </div>
                                                ) : schema.tables && schema.tables.length === 0 ? (
                                                    <div className="py-2 text-sm text-gray-500 italic">
                                                        No tables found in this schema.
                                                    </div>
                                                ) : (
                                                    <div className="grid grid-cols-1 md:grid-cols-2 gap-2 p-2">
                                                        {schema.tables?.map(table => (
                                                            <div
                                                                key={table}
                                                                className="flex items-center gap-2 p-2 rounded hover:bg-gray-900 group"
                                                            >
                                                                <Table className="h-4 w-4 text-gray-600 group-hover:text-blue-400" />
                                                                <span className="text-sm text-gray-400 group-hover:text-gray-200 truncate" title={table}>
                                                                    {table}
                                                                </span>
                                                            </div>
                                                        ))}
                                                    </div>
                                                )}
                                            </div>
                                        )}
                                    </div>
                                );
                            })}
                        </div>
                    )}
                </CardContent>
            </Card>
        </div>
    );
}
