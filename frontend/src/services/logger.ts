type LogLevel = 'info' | 'warn' | 'error' | 'debug';

class Logger {
    private static formatMessage(level: LogLevel, message: string): string {
        const timestamp = new Date().toISOString();
        return `[${timestamp}] [${level.toUpperCase()}] ${message}`;
    }

    static info(message: string, data?: any) {
        const msg = this.formatMessage('info', message);
        if (data) {
            console.log(`%c${msg}`, 'color: #4ade80', data);
        } else {
            console.log(`%c${msg}`, 'color: #4ade80');
        }
    }

    static warn(message: string, data?: any) {
        const msg = this.formatMessage('warn', message);
        if (data) {
            console.warn(`%c${msg}`, 'color: #facc15', data);
        } else {
            console.warn(`%c${msg}`, 'color: #facc15');
        }
    }

    static error(message: string, error?: any) {
        const msg = this.formatMessage('error', message);
        if (error) {
            console.error(`%c${msg}`, 'color: #f87171', error);
        } else {
            console.error(`%c${msg}`, 'color: #f87171');
        }
    }

    static debug(message: string, data?: any) {
        const msg = this.formatMessage('debug', message);
        if (data) {
            console.debug(`%c${msg}`, 'color: #94a3b8', data);
        } else {
            console.debug(`%c${msg}`, 'color: #94a3b8');
        }
    }
}

export default Logger;
