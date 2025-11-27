import { Activity } from "lucide-react";

interface LayoutProps {
    children: React.ReactNode;
}

export default function Layout({ children }: LayoutProps) {
    return (
        <div className="min-h-screen bg-gray-950 text-gray-100 font-sans selection:bg-blue-500/30">
            <header className="border-b border-gray-800 bg-gray-900/50 backdrop-blur-xl sticky top-0 z-50">
                <div className="container mx-auto px-4 h-16 flex items-center justify-between">
                    <div className="flex items-center gap-3">
                        <div className="p-2 bg-blue-600/10 rounded-lg">
                            <Activity className="h-6 w-6 text-blue-500" />
                        </div>
                        <div>
                            <h1 className="text-xl font-bold bg-gradient-to-r from-blue-400 to-cyan-400 bg-clip-text text-transparent">
                                Lakehouse Assistant
                            </h1>
                            <p className="text-xs text-gray-500 font-medium tracking-wide">
                                INTELLIGENT DATA OPS
                            </p>
                        </div>
                    </div>
                    <div className="flex items-center gap-4">
                        <div className="h-2 w-2 rounded-full bg-green-500 animate-pulse" />
                        <span className="text-xs font-medium text-gray-400">System Online</span>
                    </div>
                </div>
            </header>

            <main className="container mx-auto px-4 py-8">
                {children}
            </main>

            <footer className="border-t border-gray-800 mt-auto py-8 text-center text-sm text-gray-600">
                <p>&copy; {new Date().getFullYear()} Lakehouse Assistant. All rights reserved.</p>
            </footer>
        </div>
    );
}
