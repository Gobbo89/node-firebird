import { describe, it, expect, vi } from 'vitest';
import Events from 'events';
import net from 'net';
import * as Firebird from '../../src/index';
import Pool from '../../src/pool';
import EventConnection from '../../src/wire/eventConnection';
import { escape as utilsEscape, resolveEventHost } from '../../src/utils';

describe('public API surface', () => {
    it('exports the connection entry points', () => {
        expect(typeof Firebird.attach).toBe('function');
        expect(typeof Firebird.create).toBe('function');
        expect(typeof Firebird.attachOrCreate).toBe('function');
        expect(typeof Firebird.drop).toBe('function');
        expect(typeof Firebird.pool).toBe('function');
    });

    it('exports the auth plugin names', () => {
        expect(Firebird.AUTH_PLUGIN_LEGACY).toBe('Legacy_Auth');
        expect(Firebird.AUTH_PLUGIN_SRP).toBe('Srp');
        expect(Firebird.AUTH_PLUGIN_SRP256).toBe('Srp256');
        expect(Firebird.AUTH_PLUGIN_SRP384).toBe('Srp384');
        expect(Firebird.AUTH_PLUGIN_SRP512).toBe('Srp512');
    });

    it('exports wire crypt flags', () => {
        expect(Firebird.WIRE_CRYPT_DISABLE).toBe(0);
        expect(Firebird.WIRE_CRYPT_ENABLE).toBe(1);
    });

    it('exports isolation level arrays', () => {
        for (const iso of [
            Firebird.ISOLATION_READ_UNCOMMITTED,
            Firebird.ISOLATION_READ_COMMITTED,
            Firebird.ISOLATION_REPEATABLE_READ,
            Firebird.ISOLATION_SERIALIZABLE,
            Firebird.ISOLATION_READ_COMMITTED_READ_ONLY,
        ]) {
            expect(Array.isArray(iso)).toBe(true);
            expect(iso.length).toBeGreaterThan(0);
        }
    });

    it('escape is the utils implementation', () => {
        expect(Firebird.escape).toBe(utilsEscape);
        expect(Firebird.escape("it's")).toBe("'it''s'");
    });

    it('escape does not double backslashes (Firebird literals have no backslash escapes, #156)', () => {
        expect(Firebird.escape('a\\b')).toBe("'a\\b'");
        expect(Firebird.escape("path\\to's")).toBe("'path\\to''s'");
    });

    it('re-exports GDSCode', () => {
        expect(Firebird.GDSCode.ARITH_EXCEPT).toBe(335544321);
    });

    it('exports the FbEventManager constructor', () => {
        expect(typeof Firebird.FbEventManager).toBe('function');
    });

    it('resolves advertised, wildcard and explicitly overridden event hosts', () => {
        expect(resolveEventHost({ host: 'db.internal' }, 'events.internal')).toBe('events.internal');
        expect(resolveEventHost({ host: 'db.internal' }, '0.0.0.0')).toBe('db.internal');
        expect(resolveEventHost({ host: 'db.internal' }, '::')).toBe('db.internal');
        expect(resolveEventHost({ host: 'db.internal', eventHost: 'public.example' }, 'events.internal')).toBe('public.example');
    });

    it('exports DPB constants including isc_dpb_search_path', () => {
        expect(Firebird.isc_dpb_search_path).toBe(105);
        expect(Firebird.isc_dpb_owner).toBe(102);
        expect(Firebird.isc_dpb_parallel_workers).toBe(100);
        expect(Firebird.isc_dpb_max_inline_blob_size).toBe(104);
        expect(Firebird.isc_dpb_user_name).toBe(28);
        expect(Firebird.isc_dpb_password).toBe(29);
        expect(Firebird.isc_dpb_page_size).toBe(4);
    });

    it('pool() returns a Pool marked as pooled', () => {
        const p = Firebird.pool(3, {} as any) as any;
        expect(p).toBeInstanceOf(Pool);
        expect(p.max).toBe(3);
        expect(p.options.isPool).toBe(true);
        expect(typeof p.get).toBe('function');
        expect(typeof p.destroy).toBe('function');
    });
});

function fakeEventManager() {
    const socket = new Events.EventEmitter() as Events.EventEmitter & { destroyed: boolean; end: () => void };
    socket.destroyed = false;
    socket.end = () => {
        socket.destroyed = true;
        process.nextTick(() => socket.emit('close'));
    };
    const connection = {
        _isClosed: false,
        queEvents: vi.fn((_events: any, _id: number, callback: any) => callback(null, {})),
        closeEvents: vi.fn((_id: number, callback: any) => callback(null)),
        _emitError: vi.fn(),
    };
    const eventConnection: any = {
        _isClosed: false,
        _socket: socket,
        eventcallback: null,
        emgr: null,
        throwClosed(callback?: (err: Error) => void) {
            if (callback) callback(new Error('Event Connection is closed.'));
        },
    };
    const manager = new Firebird.FbEventManager({ connection }, eventConnection, 42);
    return { manager, connection, eventConnection };
}

describe('FbEventManager baseline state machine', () => {
    it('emits a baseline without a phantom event, then emits the first real change', () => {
        const { manager, eventConnection, connection } = fakeEventManager();
        const baselines: any[] = [];
        const posts: any[] = [];
        manager.on('baseline', counts => baselines.push(counts));
        manager.on('post_event', (name, count) => posts.push([name, count]));

        manager.registerEvent(['A'], err => expect(err).toBeFalsy());
        eventConnection.eventcallback(null, { eventid: 42, counts: { A: 5 } });
        expect(baselines).toEqual([{ A: 5 }]);
        expect(Object.isFrozen(baselines[0])).toBe(true);
        expect(posts).toEqual([]);

        eventConnection.eventcallback(null, { eventid: 42, counts: { A: 6 } });
        expect(posts).toEqual([['A', 6]]);
        expect(connection.queEvents).toHaveBeenCalledTimes(3);
    });

    it('creates a fresh baseline for register and unregister generations', () => {
        const { manager, eventConnection } = fakeEventManager();
        const baselines: any[] = [];
        manager.on('baseline', counts => baselines.push(counts));

        manager.registerEvent(['A'], () => {});
        eventConnection.eventcallback(null, { eventid: 42, counts: { A: 2 } });
        manager.registerEvent(['B'], () => {});
        eventConnection.eventcallback(null, { eventid: 42, counts: { A: 2, B: 9 } });
        manager.unregisterEvent(['A'], () => {});
        eventConnection.eventcallback(null, { eventid: 42, counts: { A: 99, B: 9 } });

        expect(baselines).toEqual([{ A: 2 }, { A: 2, B: 9 }, { B: 9 }]);
        expect(manager.getState().registeredEvents).toEqual({ B: 9 });
    });

    it('rejects empty and overlong UTF-8 event names before queuing', () => {
        const { manager, connection } = fakeEventManager();
        const errors: Error[] = [];
        manager.registerEvent([''], err => errors.push(err));
        manager.registerEvent(['€'.repeat(43)], err => errors.push(err));
        expect(errors).toHaveLength(2);
        expect(errors[0].message).toMatch(/1 and 127 UTF-8 bytes/);
        expect(connection.queEvents).not.toHaveBeenCalled();

        const boundary = fakeEventManager();
        let boundaryError: any;
        boundary.manager.registerEvent(['a'.repeat(127)], err => { boundaryError = err; });
        expect(boundaryError).toBeFalsy();
        expect(boundary.connection.queEvents).toHaveBeenCalledTimes(1);
    });

    it('closes the manager on a mismatched event id', () => {
        const { manager, eventConnection } = fakeEventManager();
        const errors: Error[] = [];
        manager.on('error', error => errors.push(error));
        manager.registerEvent(['A'], () => {});
        eventConnection.eventcallback(null, { eventid: 99, counts: { A: 1 } });
        expect(errors[0].message).toMatch(/Bad event id/);
        expect(manager.getState().state).toBe('CLOSED');
    });

    it('routes asynchronous failures to the manager or guarded database error path', () => {
        const listened = fakeEventManager();
        const errors: Error[] = [];
        listened.manager.on('error', error => errors.push(error));
        listened.manager._handleAsyncError(new Error('aux failed'));
        expect(errors.map(error => error.message)).toEqual(['aux failed']);
        expect(listened.connection._emitError).not.toHaveBeenCalled();

        const guarded = fakeEventManager();
        guarded.manager._handleAsyncError(new Error('unobserved aux failure'));
        expect(guarded.connection._emitError).toHaveBeenCalledTimes(1);
    });

    it('does not requeue after all events are removed and closes idempotently', async () => {
        const { manager, eventConnection, connection } = fakeEventManager();
        manager.registerEvent(['A'], () => {});
        eventConnection.eventcallback(null, { eventid: 42, counts: { A: 0 } });
        manager.unregisterEvent(['A'], () => {});
        const queuedBeforeLatePacket = connection.queEvents.mock.calls.length;
        eventConnection.eventcallback(null, { eventid: 42, counts: { A: 1 } });
        expect(connection.queEvents).toHaveBeenCalledTimes(queuedBeforeLatePacket);

        await new Promise<void>((resolve, reject) => manager.close(err => err ? reject(err) : resolve()));
        await new Promise<void>((resolve, reject) => manager.close(err => err ? reject(err) : resolve()));
        expect(manager.getState().state).toBe('CLOSED');
    });
});

describe('auxiliary event socket failures', () => {
    it('fails attach exactly once when the auxiliary port refuses the connection', async () => {
        const probe = net.createServer();
        await new Promise<void>(resolve => probe.listen(0, '127.0.0.1', resolve));
        const address = probe.address() as net.AddressInfo;
        await new Promise<void>((resolve, reject) => probe.close(err => err ? reject(err) : resolve()));

        let callbackCount = 0;
        const error = await new Promise<Error>(resolve => {
            new EventConnection('127.0.0.1', address.port, err => {
                callbackCount++;
                if (err) resolve(err);
            }, { connection: { _emitError: vi.fn() } });
        });
        await new Promise(resolve => setTimeout(resolve, 10));
        expect(error).toBeInstanceOf(Error);
        expect(callbackCount).toBe(1);
    });

    it('reports an unexpected post-attach close through the manager', async () => {
        let accepted: net.Socket | undefined;
        const server = net.createServer(socket => { accepted = socket; });
        await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
        const address = server.address() as net.AddressInfo;
        const db = {
            connection: {
                _isClosed: false,
                queEvents: vi.fn(),
                closeEvents: vi.fn(),
                _emitError: vi.fn(),
            },
        };

        const error = await new Promise<Error>((resolve, reject) => {
            const connection = new EventConnection('127.0.0.1', address.port, err => {
                if (err) {
                    reject(err);
                    return;
                }
                const manager = new Firebird.FbEventManager(db, connection, 7);
                manager.once('error', resolve);
                accepted!.end();
            }, db);
        });
        expect(error.message).toMatch(/closed unexpectedly/);
        await new Promise<void>((resolve, reject) => server.close(err => err ? reject(err) : resolve()));
    });
});
