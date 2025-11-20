import type { AssistantReport } from '../services/api';

export default function ReportViewer({ report }: { report: AssistantReport | null }) {
    if (!report) {
        return (
            <div className="bg-gray-800 p-6 rounded-lg shadow-lg text-center text-gray-400">
                No report yet. Run the assistant to see results.
            </div>
        );
    }

    return (
        <div className="bg-gray-800 p-6 rounded-lg shadow-lg">
            <h2 className="text-2xl font-bold mb-4">Results</h2>

            <div className="mb-4">
                <p className="text-sm text-gray-400">Data Root: {report.data_root || 'N/A'}</p>
                <p className="text-sm text-gray-400">Total Datasets: {report.datasets.length}</p>
            </div>

            <div className="space-y-4">
                {report.datasets.map((dataset, idx) => (
                    <div key={idx} className="bg-gray-700 p-4 rounded">
                        <h3 className="font-bold text-lg mb-2">{dataset.name}</h3>
                        <div className="grid grid-cols-2 gap-2 text-sm">
                            <div>Rows: {dataset.rows}</div>
                            <div>Columns: {dataset.cols}</div>
                            <div>Mapped: {Object.keys(dataset.mapping).length}</div>
                            <div>Unmapped: {dataset.unmapped.length}</div>
                        </div>

                        {Object.keys(dataset.anomalies).length > 0 && (
                            <div className="mt-2">
                                <p className="font-semibold">Anomalies:</p>
                                <ul className="text-sm">
                                    {Object.entries(dataset.anomalies).map(([method, count]) => (
                                        <li key={method}>{method}: {count}</li>
                                    ))}
                                </ul>
                            </div>
                        )}
                    </div>
                ))}
            </div>
        </div>
    );
}
