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

                                    <div className="space-y-2">
                                        <div className="flex justify-between text-xs text-gray-500 uppercase font-medium">
                                            <span>Mapped Fields</span>
                                            <span>{Object.keys(dataset.mapping).length} / {dataset.cols}</span>
                                        </div>
                                        <div className="h-2 bg-gray-800 rounded-full overflow-hidden">
                                            <div
                                                className="h-full bg-blue-600 rounded-full"
                                                style={{ width: `${(Object.keys(dataset.mapping).length / dataset.cols) * 100}%` }}
                                            />
                                        </div>
                                    </div>

                                    {Object.keys(dataset.mapping).length > 0 && (
                                        <div className="mt-4 grid grid-cols-1 md:grid-cols-2 gap-2">
                                            {Object.entries(dataset.mapping).map(([field, match]: [string, any]) => (
                                                <div key={field} className="flex items-center justify-between text-sm p-2 bg-gray-800/50 rounded">
                                                    <span className="text-gray-300">{field}</span>
                                                    <div className="flex items-center gap-2">
                                                        <span className="text-gray-500">→</span>
                                                        <Badge variant="secondary">
                                                            {typeof match === 'object' && match !== null ? match.target : match}
                                                        </Badge>
                                                    </div>
                                                </div>
                                            ))}
                                        </div>
                                    )}
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
                                        <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-4">
                                            {Object.entries(dataset.anomalies).map(([method, count]) => (
                                                <div key={method} className="bg-gray-900/50 p-3 rounded border border-gray-800">
                                                    <div className="text-xs text-gray-500 uppercase mb-1">{method}</div>
                                                    <div className="text-xl font-bold text-gray-200">{count}</div>
                                                </div>
                                            ))}
                                        </div>
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
