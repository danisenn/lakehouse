import { useState } from 'react';
import ConfigPanel from './components/ConfigPanel';
import ReportViewer from './components/ReportViewer';
import type { AssistantReport } from './services/api';

function App() {
    const [report, setReport] = useState<AssistantReport | null>(null);

    return (
        <div className="min-h-screen bg-gray-900 text-white p-8">
            <div className="max-w-7xl mx-auto">
                <header className="mb-8">
                    <h1 className="text-4xl font-bold mb-2">Lakehouse Assistant</h1>
                    <p className="text-gray-400">
                        AI-Powered Data Integration & Quality Analysis
                    </p>
                </header>

                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    <ConfigPanel onRunComplete={setReport} />
                    <ReportViewer report={report} />
                </div>
            </div>
        </div>
    );
}

export default App;
