import Events from 'events';

type Completion = (err?: any, result?: any) => void;
type EventPacket = { eventid: number; counts: Record<string, number> };

function once(callback?: Completion): Completion {
    var called = false;
    return function(err?: any, result?: any) {
        if (called) return;
        called = true;
        if (callback) callback(err, result);
    };
}

/** Stable diagnostic state returned by {@link FbEventManager.getState}. */
interface FbEventState {
    state: 'IDLE' | 'SUBSCRIBED' | 'CLOSED';
    hasActiveSubscription: boolean;
    registeredEvents: Record<string, number>;
    eventId: number;
    isEventConnectionOpen: boolean;
    isDatabaseConnectionClosed: boolean;
}

/** Manages Firebird POST_EVENT subscriptions over an auxiliary connection. */
class FbEventManager extends Events.EventEmitter {
    db: any;
    eventconnection: any;
    events: Record<string, number>;
    eventid: number;
    _subscriptionVersion: number;
    _hasActiveSubscription: boolean;
    _baselinePending: boolean;
    _closed: boolean;
    _closing: boolean;
    _closeCallbacks: Completion[];

    constructor(db: any, eventconnection: any, eventid: number) {
        super();
        this.db = db;
        this.eventconnection = eventconnection;
        this.events = {};
        this.eventid = eventid;
        this._subscriptionVersion = 0;
        this._hasActiveSubscription = false;
        this._baselinePending = false;
        this._closed = false;
        this._closing = false;
        this._closeCallbacks = [];
        this._createEventLoop();
    }

    on(event: 'baseline', listener: (counts: Readonly<Record<string, number>>) => void): this;
    on(event: 'post_event', listener: (name: string, count: number) => void): this;
    on(event: 'error', listener: (error: Error) => void): this;
    on(event: string | symbol, listener: (...args: any[]) => void): this {
        return super.on(event, listener);
    }

    once(event: 'baseline', listener: (counts: Readonly<Record<string, number>>) => void): this;
    once(event: 'post_event', listener: (name: string, count: number) => void): this;
    once(event: 'error', listener: (error: Error) => void): this;
    once(event: string | symbol, listener: (...args: any[]) => void): this {
        return super.once(event, listener);
    }

    getState(): FbEventState {
        const eventConnectionOpen = Boolean(this.eventconnection && !this.eventconnection._isClosed && !this._closed && !this._closing);
        const databaseConnectionClosed = !this.db.connection || this.db.connection._isClosed;
        let state: FbEventState['state'];
        if (!eventConnectionOpen || databaseConnectionClosed) state = 'CLOSED';
        else if (this._hasActiveSubscription) state = 'SUBSCRIBED';
        else state = 'IDLE';

        return {
            state,
            hasActiveSubscription: this._hasActiveSubscription,
            registeredEvents: { ...this.events },
            eventId: this.eventid,
            isEventConnectionOpen: eventConnectionOpen,
            isDatabaseConnectionClosed: databaseConnectionClosed,
        };
    }

    _createEventLoop(): void {
        this.eventconnection.emgr = this;
        this.eventconnection.eventcallback = (err: any, packet?: EventPacket) => {
            if (err) {
                this._handleAsyncError(err);
                return;
            }
            if (!packet || packet.eventid !== this.eventid) {
                this._handleAsyncError(new Error('Bad event id on auxiliary connection.'));
                return;
            }
            if (this._closed || this._closing || !this._hasActiveSubscription) return;

            const version = this._subscriptionVersion;
            if (this._baselinePending) {
                this._baselinePending = false;
                for (const [name, count] of Object.entries(packet.counts)) {
                    if (Object.prototype.hasOwnProperty.call(this.events, name)) this.events[name] = count;
                }
                this.emit('baseline', Object.freeze({ ...this.events }));
            } else {
                for (const [name, count] of Object.entries(packet.counts)) {
                    if (!Object.prototype.hasOwnProperty.call(this.events, name)) continue;
                    const previous = this.events[name];
                    this.events[name] = count;
                    if (previous !== count) this.emit('post_event', name, count);
                }
            }

            this._requeue(version);
        };
    }

    _requeue(version: number): void {
        if (this._closed || this._closing || !this._hasActiveSubscription ||
            version !== this._subscriptionVersion || Object.keys(this.events).length === 0) return;

        this.db.connection.queEvents(this.events, this.eventid, (err: any) => {
            if (err && version === this._subscriptionVersion && !this._closed) this._handleAsyncError(err);
        });
    }

    _changeEvent(callback?: Completion): void {
        const done = once(callback);
        const version = ++this._subscriptionVersion;
        const hadActiveSubscription = this._hasActiveSubscription;
        this._hasActiveSubscription = false;
        this._baselinePending = false;

        const subscribe = () => {
            if (version !== this._subscriptionVersion || this._closed || this._closing) {
                done(this._closed || this._closing ? new Error('Event Connection is closed.') : undefined);
                return;
            }
            if (Object.keys(this.events).length === 0) {
                done();
                return;
            }

            // A reconfiguration is a new baseline generation. Firebird only
            // wakes an event request immediately when the supplied counters
            // differ from its current counters, so reset the request values
            // instead of carrying the previous generation forward.
            for (const name of Object.keys(this.events)) this.events[name] = 0;
            this._hasActiveSubscription = true;
            this._baselinePending = true;
            this.db.connection.queEvents(this.events, this.eventid, (err: any, result?: any) => {
                if (err && version === this._subscriptionVersion) {
                    this._hasActiveSubscription = false;
                    this._baselinePending = false;
                }
                done(err, result);
            });
        };

        if (!hadActiveSubscription) {
            subscribe();
            return;
        }

        this.db.connection.closeEvents(this.eventid, (err: any) => {
            if (err) {
                done(err);
                return;
            }
            subscribe();
        });
    }

    registerEvent(eventNames: string[], callback?: Completion): this {
        if (this.db.connection._isClosed || this.eventconnection._isClosed || this._closed || this._closing) {
            this.eventconnection.throwClosed(callback);
            return this;
        }

        for (const name of eventNames) {
            const byteLength = typeof name === 'string' ? Buffer.byteLength(name, 'utf8') : 0;
            if (byteLength < 1 || byteLength > 127) {
                if (callback) callback(new Error('Firebird event names must be between 1 and 127 UTF-8 bytes.'));
                return this;
            }
        }
        for (const name of eventNames) this.events[name] = this.events[name] || 0;
        this._changeEvent(callback);
        return this;
    }

    unregisterEvent(eventNames: string[], callback?: Completion): this {
        if (this.db.connection._isClosed || this.eventconnection._isClosed || this._closed || this._closing) {
            this.eventconnection.throwClosed(callback);
            return this;
        }
        for (const name of eventNames) delete this.events[name];
        this._changeEvent(callback);
        return this;
    }

    _handleAsyncError(err: any): void {
        if (this._closed) return;
        const error = err instanceof Error ? err : new Error(String(err));
        this._closed = true;
        this._closing = false;
        this._hasActiveSubscription = false;
        this._baselinePending = false;
        this._subscriptionVersion++;
        this.eventconnection._isClosed = true;
        this.eventconnection.eventcallback = null;
        if (this.eventconnection._socket && !this.eventconnection._socket.destroyed &&
            typeof this.eventconnection._socket.destroy === 'function') this.eventconnection._socket.destroy();

        const closeCallbacks = this._closeCallbacks.splice(0);
        for (const callback of closeCallbacks) callback(error);

        if (this.listenerCount('error') > 0) this.emit('error', error);
        else if (this.db.connection && typeof this.db.connection._emitError === 'function') this.db.connection._emitError(error);
    }

    close(callback?: Completion): this {
        const done = once(callback);
        if (this._closed) {
            process.nextTick(() => done());
            return this;
        }
        if (this._closing) {
            this._closeCallbacks.push(done);
            return this;
        }

        this._closing = true;
        this._closeCallbacks.push(done);
        this._subscriptionVersion++;
        const hadActiveSubscription = this._hasActiveSubscription;
        this._hasActiveSubscription = false;
        this._baselinePending = false;
        this.eventconnection.eventcallback = null;

        const finish = (cancelError?: any) => {
            const socket = this.eventconnection && this.eventconnection._socket;
            let settled = false;
            let timer: NodeJS.Timeout | undefined;
            const complete = () => {
                if (settled) return;
                settled = true;
                if (timer) clearTimeout(timer);
                this._closing = false;
                this._closed = true;
                this.eventconnection._isClosed = true;
                const callbacks = this._closeCallbacks.splice(0);
                for (const closeCallback of callbacks) closeCallback(cancelError);
            };

            if (!socket || socket.destroyed) {
                complete();
                return;
            }
            socket.once('close', complete);
            socket.end();
            timer = setTimeout(complete, 200);
        };

        if (!hadActiveSubscription || this.db.connection._isClosed) {
            finish();
            return this;
        }
        this.db.connection.closeEvents(this.eventid, (err: any) => finish(err));
        return this;
    }
}

export = FbEventManager;
