import { Info } from 'lucide-react';
import { useState } from 'react';

interface InfoTooltipProps {
    content: string;
    className?: string;
}

export function InfoTooltip({ content, className = '' }: InfoTooltipProps) {
    const [isVisible, setIsVisible] = useState(false);

    return (
        <div
            className={`relative inline-flex items-center ${className}`}
            onMouseEnter={() => setIsVisible(true)}
            onMouseLeave={() => setIsVisible(false)}
        >
            <Info
                className="h-4 w-4 text-gray-500 hover:text-blue-400 cursor-help transition-colors"
            />

            {isVisible && (
                <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 w-64 p-2 bg-gray-900 border border-gray-700 rounded-lg shadow-xl z-50 animate-in fade-in zoom-in-95 duration-200">
                    <p className="text-xs text-gray-200 leading-relaxed text-center">
                        {content}
                    </p>
                    {/* Arrow */}
                    <div className="absolute top-full left-1/2 -translate-x-1/2 -mt-1 border-4 border-transparent border-t-gray-700" />
                </div>
            )}
        </div>
    );
}
