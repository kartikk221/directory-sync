import { abort_error } from './operators.js';

/**
 * Bounded asynchronous work queue with concurrency, queue-wait timeout, and
 * sliding-window start-rate controls. It stores only queued handlers and recent
 * start timestamps; completed work is released immediately.
 */
class TaskQueue {
    #active = 0;
    #closed = false;
    #concurrency;
    #maximum;
    #pending = [];
    #pending_offset = 0;
    #pending_size = 0;
    #started = [];
    #started_offset = 0;
    #throttle_interval;
    #throttle_rate;
    #throttle_timer;
    #timeout;

    /**
     * @param {number|{concurrency?: number, maximum?: number, timeout?: number, throttle?: {rate?: number, interval?: number}}} [concurrency=1] Concurrency or complete options.
     * @param {number} [maximum=Infinity] Maximum waiting tasks; active tasks are not counted.
     * @param {number} [timeout=Infinity] Maximum queue wait in milliseconds.
     */
    constructor(concurrency = 1, maximum = Infinity, timeout = Infinity) {
        let throttle = {};
        if (typeof concurrency === 'object') {
            ({ concurrency = 1, maximum = Infinity, timeout = Infinity, throttle = {} } = concurrency);
        }
        if (!Number.isSafeInteger(concurrency) || concurrency < 1)
            throw new TypeError('TaskQueue concurrency must be a positive safe integer.');
        if (maximum !== Infinity && (!Number.isSafeInteger(maximum) || maximum < 0))
            throw new TypeError('TaskQueue maximum must be a non-negative safe integer or Infinity.');
        if (timeout !== Infinity && (!Number.isFinite(timeout) || timeout < 0))
            throw new TypeError('TaskQueue timeout must be a non-negative number or Infinity.');
        const rate = throttle.rate ?? Infinity;
        const interval = throttle.interval ?? Infinity;
        if (rate !== Infinity && (!Number.isSafeInteger(rate) || rate < 1))
            throw new TypeError('TaskQueue throttle.rate must be a positive safe integer or Infinity.');
        if (rate !== Infinity && (!Number.isFinite(interval) || interval <= 0))
            throw new TypeError('TaskQueue throttle.interval must be a positive number when throttling.');
        this.#concurrency = concurrency;
        this.#maximum = maximum;
        this.#timeout = timeout;
        this.#throttle_rate = rate;
        this.#throttle_interval = interval;
    }

    /**
     * Schedules work subject to capacity and throttle limits.
     *
     * @template T
     * @param {() => T|Promise<T>} handler Work to execute.
     * @param {AbortSignal} [signal] Cancels work while it is still queued.
     * @returns {Promise<T>}
     */
    run(handler, signal) {
        if (this.#closed || signal?.aborted) return Promise.reject(abort_error());
        if (this.#active >= this.#concurrency && this.#pending_size >= this.#maximum) {
            const error = new Error('Task queue capacity was exceeded.');
            error.code = 'QUEUE_FULL';
            return Promise.reject(error);
        }
        return new Promise((resolve, reject) => {
            const item = { cancelled: false, handler, reject, resolve, signal, timer: undefined };
            if (this.#timeout !== Infinity) {
                item.timer = setTimeout(() => {
                    if (item.cancelled) return;
                    item.cancelled = true;
                    this.#pending_size--;
                    const error = new Error('Task queue wait timed out.');
                    error.code = 'QUEUE_TIMEOUT';
                    reject(error);
                }, this.#timeout);
                item.timer.unref?.();
            }
            this.#pending.push(item);
            this.#pending_size++;
            this._drain();
        });
    }

    _drain() {
        while (!this.#closed && this.#active < this.#concurrency && this.#pending_size) {
            if (!this._throttle()) return;
            let item;
            while (this.#pending_offset < this.#pending.length && !item) {
                const candidate = this.#pending[this.#pending_offset++];
                if (!candidate.cancelled) item = candidate;
            }
            if (!item) break;
            this.#pending_size--;
            item.cancelled = true;
            if (item.timer) clearTimeout(item.timer);
            if (item.signal?.aborted) {
                item.reject(abort_error());
                continue;
            }
            this.#active++;
            Promise.resolve()
                .then(item.handler)
                .then((value) => {
                    this.#active--;
                    this._drain();
                    item.resolve(value);
                }, (error) => {
                    this.#active--;
                    this._drain();
                    item.reject(error);
                });
        }
        if (this.#pending_offset > 1_024 && this.#pending_offset * 2 > this.#pending.length) {
            this.#pending = this.#pending.slice(this.#pending_offset);
            this.#pending_offset = 0;
        }
    }

    _throttle() {
        if (this.#throttle_rate === Infinity) return true;
        const now = Date.now();
        const cutoff = now - this.#throttle_interval;
        while (
            this.#started_offset < this.#started.length &&
            this.#started[this.#started_offset] <= cutoff
        ) this.#started_offset++;
        if (this.#started.length - this.#started_offset < this.#throttle_rate) {
            this.#started.push(now);
            if (this.#started_offset > 1_024 && this.#started_offset * 2 > this.#started.length) {
                this.#started = this.#started.slice(this.#started_offset);
                this.#started_offset = 0;
            }
            return true;
        }
        if (!this.#throttle_timer) {
            const delay = Math.max(1, this.#started[this.#started_offset] + this.#throttle_interval - now);
            this.#throttle_timer = setTimeout(() => {
                this.#throttle_timer = undefined;
                this._drain();
            }, delay);
        }
        return false;
    }

    /**
     * Rejects queued work and prevents future scheduling. Active work is allowed to finish.
     *
     * @param {Error} [error] Rejection used for queued work.
     * @returns {void}
     */
    close(error = abort_error()) {
        if (this.#closed) return;
        this.#closed = true;
        if (this.#throttle_timer) clearTimeout(this.#throttle_timer);
        this.#throttle_timer = undefined;
        for (let index = this.#pending_offset; index < this.#pending.length; index++) {
            const item = this.#pending[index];
            if (item.cancelled) continue;
            item.cancelled = true;
            if (item.timer) clearTimeout(item.timer);
            item.reject(error);
        }
        this.#pending = [];
        this.#pending_offset = 0;
        this.#pending_size = 0;
    }

    /** @returns {number} Number of tasks waiting to start. */
    get size() {
        return this.#pending_size;
    }
}

/** Serializes work per key while allowing independent keys to run concurrently. */
class KeyedQueue {
    #closed = false;
    #tails = new Map();

    /**
     * @template T
     * @param {string} key Serialization key, normally a canonical URI.
     * @param {() => T|Promise<T>} handler Work to execute.
     * @returns {Promise<T>}
     */
    run(key, handler) {
        if (this.#closed) return Promise.reject(abort_error());
        const previous = this.#tails.get(key) || Promise.resolve();
        const current = previous.catch(() => undefined).then(() => {
            if (this.#closed) throw abort_error();
            return handler();
        });
        this.#tails.set(key, current);
        current.finally(() => {
            if (this.#tails.get(key) === current) this.#tails.delete(key);
        }).catch(() => undefined);
        return current;
    }

    /** @returns {Promise<void>} Resolves after all current key tails settle. */
    async idle() {
        await Promise.allSettled([...this.#tails.values()]);
    }

    /** @returns {void} Prevents future work from being scheduled. */
    close() {
        this.#closed = true;
    }
}

export { KeyedQueue, TaskQueue };
