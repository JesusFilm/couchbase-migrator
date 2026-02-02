/**
 * Logger utility for conditional debug logging
 */

export class Logger {
  private debug: boolean

  constructor(debug: boolean = false) {
    this.debug = debug
  }

  log(...args: unknown[]): void {
    if (this.debug) {
      console.log(...args)
    }
  }

  warn(...args: unknown[]): void {
    if (this.debug) {
      console.warn(...args)
    }
  }

  error(...args: unknown[]): void {
    console.error(...args)
  }

  info(...args: unknown[]): void {
    console.log(...args)
  }
}
