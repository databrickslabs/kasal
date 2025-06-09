/**
 * Logger utility for centralized logging management
 * This helps control log output across the application and easily enable/disable logs
 * based on environment or configurations.
 */

type LogLevel = 'debug' | 'info' | 'warn' | 'error';

interface LoggerOptions {
  enabled: boolean;
  level: LogLevel;
  prefix?: string;
}

// Default options
const defaultOptions: LoggerOptions = {
  enabled: process.env.NODE_ENV !== 'production',
  level: 'info',
};

// Log level priorities (higher number = higher priority)
const LOG_LEVELS: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
};

class Logger {
  private options: LoggerOptions;

  constructor(options: Partial<LoggerOptions> = {}) {
    this.options = { ...defaultOptions, ...options };
  }

  /**
   * Check if logging is enabled for a specific level
   */
  private shouldLog(level: LogLevel): boolean {
    if (!this.options.enabled) return false;
    return LOG_LEVELS[level] >= LOG_LEVELS[this.options.level];
  }

  /**
   * Format the message with prefix if configured
   */
  private formatMessage(message: string): string {
    return this.options.prefix ? `[${this.options.prefix}] ${message}` : message;
  }

  /**
   * Debug level logging
   */
  debug(...args: unknown[]): void {
    if (this.shouldLog('debug')) {
      if (typeof args[0] === 'string') {
        console.debug(this.formatMessage(args[0]), ...args.slice(1));
      } else {
        console.debug(...args);
      }
    }
  }

  /**
   * Info level logging
   */
  info(...args: unknown[]): void {
    if (this.shouldLog('info')) {
      if (typeof args[0] === 'string') {
        console.info(this.formatMessage(args[0]), ...args.slice(1));
      } else {
        console.info(...args);
      }
    }
  }

  /**
   * Warning level logging
   */
  warn(...args: unknown[]): void {
    if (this.shouldLog('warn')) {
      if (typeof args[0] === 'string') {
        console.warn(this.formatMessage(args[0]), ...args.slice(1));
      } else {
        console.warn(...args);
      }
    }
  }

  /**
   * Error level logging
   */
  error(...args: unknown[]): void {
    if (this.shouldLog('error')) {
      if (typeof args[0] === 'string') {
        console.error(this.formatMessage(args[0]), ...args.slice(1));
      } else {
        console.error(...args);
      }
    }
  }

  /**
   * Create a child logger with a specific prefix
   */
  createChild(prefix: string): Logger {
    return new Logger({
      ...this.options,
      prefix: this.options.prefix
        ? `${this.options.prefix}:${prefix}`
        : prefix,
    });
  }

  /**
   * Update logger options
   */
  setOptions(options: Partial<LoggerOptions>): void {
    this.options = { ...this.options, ...options };
  }
}

// Export a default logger instance
export const logger = new Logger();

// Export the Logger class for creating custom instances
export default Logger; 