import { BarChart, FileText, AlertTriangle, CheckCircle, Database } from 'lucide-react';
import type { AssistantReport } from '../services/api';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from './ui/Card';
import { Tabs, TabsContent, TabsList, TabsTrigger } from './ui/Tabs';
import { Badge } from './ui/Badge';

export default function ReportViewer({ report }: { report: AssistantReport | null }) {
    if (!report) {
        return (
            <Card className="h-full flex items-center justify-center bg-gray-900/50 border-gray-800 border-dashed">
                <div className="text-center space-y-4 p-8">
                    <div className="bg-gray-800 p-4 rounded-full inline-block">
                        <BarChart className="h-8 w-8 text-gray-500" />
                    </div>
                    <h3 className="text-xl font-medium text-gray-300">No Analysis Results</h3>
                    <p className="text-gray-500 max-w-sm mx-auto">
                        Run the assistant to generate a quality report and integration analysis for your data.
                    </p>
                </div>
            </Card>
        );
    }

    const totalRows = report.datasets.reduce((acc, ds) => acc + ds.rows, 0);
    const totalCols = report.datasets.reduce((acc, ds) => acc + ds.cols, 0);
    const totalAnomalies = report.datasets.reduce((acc, ds) =>
        acc + Object.values(ds.anomalies).reduce((a, b) => a + b, 0), 0
    );

    return (
        <div className="space-y-6">
            {/* Summary Cards */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                <Card className="bg-gray-900/50 border-gray-800">
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium text-gray-400">Total Datasets</CardTitle>
                        <Database className="h-4 w-4 text-blue-500" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">{report.datasets.length}</div>
                        <p className="text-xs text-gray-500 mt-1">
                            Root: <span className="font-mono text-gray-400">{report.data_root || 'SQL'}</span>
                        </p>
                    </CardContent>
                </Card>
                <Card className="bg-gray-900/50 border-gray-800">
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium text-gray-400">Total Rows</CardTitle>
                        <FileText className="h-4 w-4 text-green-500" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">{totalRows.toLocaleString()}</div>
                        <p className="text-xs text-gray-500 mt-1">Across all tables</p>
                    </CardContent>
                </Card>
                <Card className="bg-gray-900/50 border-gray-800">
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium text-gray-400">Total Columns</CardTitle>
                        <FileText className="h-4 w-4 text-purple-500" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">{totalCols.toLocaleString()}</div>
                        <p className="text-xs text-gray-500 mt-1">Analyzed fields</p>
                    </CardContent>
                </Card>
                <Card className="bg-gray-900/50 border-gray-800">
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium text-gray-400">Anomalies Found</CardTitle>
                        <AlertTriangle className="h-4 w-4 text-red-500" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">{totalAnomalies}</div>
                        <p className="text-xs text-gray-500 mt-1">Potential issues</p>
                    </CardContent>
                </Card>
            </div>

            {/* Detailed View */}
            <Card className="border-gray-800 bg-gray-900/30">
                <CardHeader>
                    <CardTitle>Detailed Analysis</CardTitle>
                    <CardDescription>Deep dive into dataset quality and mapping results.</CardDescription>
                </CardHeader>
                <CardContent>
                    <Tabs defaultValue="datasets">
                        <TabsList className="grid w-full grid-cols-2 lg:w-[400px]">
                            <TabsTrigger value="datasets">Datasets</TabsTrigger>
                            <TabsTrigger value="anomalies">Anomalies</TabsTrigger>
                        </TabsList>

                        <TabsContent value="datasets" className="space-y-4 mt-4">
                            {report.datasets.map((dataset, idx) => (
                                <div key={idx} className="p-4 rounded-lg border border-gray-800 bg-gray-900/50 hover:bg-gray-900 transition-colors">
                                    <div className="flex items-start justify-between mb-4">
                                        <div>
                                            <h4 className="font-semibold text-lg flex items-center gap-2">
                                                {dataset.name}
                                                <Badge variant="outline" className="text-xs font-normal">
                                                    {dataset.rows} rows
                                                </Badge>
                                            </h4>
                                            <p className="text-sm text-gray-400 font-mono mt-1">{dataset.path}</p>
                                        </div>
                                        <div className="text-right">
                                            <div className="text-sm font-medium text-gray-300">Mapping Coverage</div>
                                            <div className="text-2xl font-bold text-blue-400">
                                                {Math.round((Object.keys(dataset.mapping).length / dataset.cols) * 100)}%
                                            </div>
                                        </div>
                                    </div>

                                    {/* LLM Summary */}
                                    {dataset.llm_insights?.summary && (
                                        <div className="mb-4 p-3 bg-blue-900/20 border border-blue-800 rounded-lg text-sm text-blue-200">
                                            <span className="font-semibold mr-2">✨ AI Summary:</span>
                                            {dataset.llm_insights.summary}
                                        </div>
                                    )}

                                    {/* Column Analysis Table */}
                                    <div className="overflow-x-auto">
                                        <table className="w-full text-sm text-left text-gray-400">
                                            <thead className="text-xs text-gray-500 uppercase bg-gray-800/50">
                                                <tr>
                                                    <th className="px-4 py-2 rounded-tl-lg">Column</th>
                                                    <th className="px-4 py-2">Description</th>
                                                    <th className="px-4 py-2">Type</th>
                                                    <th className="px-4 py-2">Semantic</th>
                                                    <th className="px-4 py-2">Completeness</th>
                                                    <th className="px-4 py-2 rounded-tr-lg">Mapping</th>
                                                </tr>
                                            </thead>
                                            <tbody>
                                                {Object.entries(dataset.schema).map(([col, dtype]) => {
                                                    const semantic = dataset.semantic_types?.[col];
                                                    const missing = dataset.statistics?.missing_ratios?.[col] || 0;
                                                    const isNested = dataset.nested_structures?.includes(col);
                                                    const isCategorical = dataset.categorical_cols?.includes(col);
                                                    const mapping = dataset.mapping[col];
                                                    const description = dataset.llm_insights?.descriptions?.[col];

                                                    // Stats
                                                    const numStats = dataset.statistics?.numeric_stats?.[col];
                                                    const textStats = dataset.statistics?.text_stats?.[col];

                                                    return (
                                                        <tr key={col} className="border-b border-gray-800 hover:bg-gray-800/30">
                                                            <td className="px-4 py-2 font-medium text-gray-300">
                                                                <div className="flex items-center gap-2">
                                                                    {col}
                                                                    {isNested && <Badge variant="outline" className="text-[10px] px-1 py-0 border-yellow-600 text-yellow-500">Nested</Badge>}
                                                                    {isCategorical && <Badge variant="outline" className="text-[10px] px-1 py-0 border-purple-600 text-purple-400">Categorical</Badge>}
                                                                </div>
                                                                {/* Detailed Stats Display */}
                                                                {numStats && (
                                                                    <div className="text-[10px] text-gray-500 mt-1 font-mono">
                                                                        μ:{numStats.mean.toFixed(2)} min:{numStats.min} max:{numStats.max}
                                                                    </div>
                                                                )}
                                                                {textStats && (
                                                                    <div className="text-[10px] text-gray-500 mt-1 font-mono">
                                                                        {textStats.unique_count} unique values
                                                                    </div>
                                                                )}
                                                            </td>
                                                            <td className="px-4 py-2 text-xs text-gray-400 italic max-w-xs truncate">
                                                                {description || <span className="text-gray-600">-</span>}
                                                            </td>
                                                            <td className="px-4 py-2 font-mono text-xs">{dtype}</td>
                                                            <td className="px-4 py-2">
                                                                {semantic && <Badge variant="success" className="text-[10px] bg-green-900/30 text-green-400 border-green-800">{semantic}</Badge>}
                                                            </td>
                                                            <td className="px-4 py-2">
                                                                <div className="flex items-center gap-2">
                                                                    <div className="w-16 h-1.5 bg-gray-700 rounded-full overflow-hidden">
                                                                        <div
                                                                            className={`h-full rounded-full ${missing > 0.5 ? 'bg-red-500' : missing > 0.1 ? 'bg-yellow-500' : 'bg-green-500'}`}
                                                                            style={{ width: `${(1 - missing) * 100}%` }}
                                                                        />
                                                                    </div>
                                                                    <span className="text-xs">{Math.round((1 - missing) * 100)}%</span>
                                                                </div>
                                                            </td>
                                                            <td className="px-4 py-2">
                                                                {mapping ? (
                                                                    <div className="flex items-center gap-1">
                                                                        <span className="text-gray-500">→</span>
                                                                        <Badge variant="secondary" className="text-xs">
                                                                            {typeof mapping === 'object' ? mapping.target : mapping}
                                                                        </Badge>
                                                                    </div>
                                                                ) : (
                                                                    <span className="text-gray-600">-</span>
                                                                )}
                                                            </td>
                                                        </tr>
                                                    );
                                                })}
                                            </tbody>
                                        </table>
                                    </div>
                                </div>
                            ))}
                        </TabsContent>

                        <TabsContent value="anomalies" className="space-y-4 mt-4">
                            {report.datasets.filter(d => Object.keys(d.anomalies).length > 0).length === 0 ? (
                                <div className="text-center py-12 text-gray-500">
                                    <CheckCircle className="h-12 w-12 mx-auto mb-4 text-green-500/20" />
                                    <p>No anomalies detected in any dataset.</p>
                                </div>
                            ) : (
                                report.datasets.filter(d => Object.keys(d.anomalies).length > 0).map((dataset, idx) => (
                                    <div key={idx} className="p-4 rounded-lg border border-red-900/30 bg-red-900/10">
                                        <h4 className="font-semibold text-red-400 mb-3 flex items-center gap-2">
                                            <AlertTriangle className="h-4 w-4" />
                                            {dataset.name}
                                        </h4>
                                        {/* Anomaly Stats */}
                                        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
                                            {Object.entries(dataset.anomalies).map(([method, count]) => (
                                                <div key={method} className="bg-gray-800/50 p-3 rounded-lg border border-gray-700">
                                                    <div className="text-xs text-gray-400 uppercase mb-1">{method}</div>
                                                    <div className="text-2xl font-bold text-red-400">{count}</div>
                                                    {dataset.anomaly_samples_saved?.[method] && (
                                                        <div className="text-[10px] text-gray-500 mt-1 truncate">
                                                            Saved to CSV
                                                        </div>
                                                    )}
                                                </div>
                                            ))}
                                        </div>

                                        {/* AI Anomaly Explanation */}
                                        {dataset.llm_insights?.anomaly_explanation && (
                                            <div className="mb-6 p-4 bg-red-900/20 border border-red-800/50 rounded-lg">
                                                <h5 className="text-sm font-semibold text-red-300 mb-2 flex items-center gap-2">
                                                    <span>🤖</span> AI Anomaly Analysis
                                                </h5>
                                                <p className="text-sm text-red-200/80 leading-relaxed">
                                                    {dataset.llm_insights.anomaly_explanation}
                                                </p>
                                            </div>
                                        )}
                                    </div>
                                ))
                            )}
                        </TabsContent>
                    </Tabs>
                </CardContent>
            </Card>
        </div>
    );
}
