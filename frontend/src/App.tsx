import { useState } from 'react';
import { Toaster } from 'react-hot-toast';
import ConfigPanel from './components/ConfigPanel';
import ReportViewer from './components/ReportViewer';
import Layout from './components/Layout';
import type { AssistantReport } from './services/api';

function App() {
    const [report, setReport] = useState<AssistantReport | null>(null);

    return (
        <Layout>
            <Toaster
                position="top-right"
                toastOptions={{
                    style: {
                        background: '#1f2937',
                        color: '#fff',
                        border: '1px solid #374151',
                    },
                }}
            />
            <div className="grid grid-cols-1 lg:grid-cols-12 gap-8">
                <div className="lg:col-span-4">
                    <ConfigPanel onRunComplete={setReport} />
                </div>
                <div className="lg:col-span-8">
                    <ReportViewer report={report} />
                </div>
            </div>
        </Layout>
    );
}

export default App;
