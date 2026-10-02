import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { BUCKET_SCAN_CONCURRENCY } from '../../constants';
import { createSandboxId } from '../../ids';
import { paths } from '../../paths';
import type { Session } from '../../schema';
import { ACTOR, makeSession, MemoryBucket, uid } from '../../testing';
import { SessionService } from './SessionService';

describe('SessionService cap scan', () => {
	let bucket: MemoryBucket;
	let sessions: SessionService;
	const metrics = { increment: vi.fn(), gauge: vi.fn(), histogram: vi.fn() };
	const key = (session: Session) => paths.session(session.project_id, session.session_id);
	const seed = async (overrides: Partial<Session> = {}) => {
		const session = makeSession(overrides);
		await bucket.put(key(session), JSON.stringify(session));
		return session;
	};

	beforeEach(() => {
		bucket = new MemoryBucket();
		sessions = new SessionService(bucket, metrics);
		vi.clearAllMocks();
	});
	afterEach(() => {
		vi.restoreAllMocks();
		vi.useRealTimers();
	});

	it('reads cold history, then skips unchanged terminal records across list pages', async () => {
		await Promise.all(Array.from({ length: 1005 }, () => seed({ status: 'terminated' })));
		const live = await seed();
		const otherUser = await seed({ user_id: uid('other') });
		const get = vi.spyOn(bucket, 'get');
		const list = vi.spyOn(bucket, 'list');

		expect(await sessions.listActiveForUser(ACTOR)).toEqual([live]);
		expect(get).toHaveBeenCalledTimes(1007);
		expect(list).toHaveBeenCalledTimes(2);
		get.mockClear();
		list.mockClear();

		expect(await sessions.listActiveForUser(ACTOR)).toEqual([live]);
		expect(get.mock.calls.map(([path]) => path).sort()).toEqual([key(live), key(otherUser)].sort());
		expect(list).toHaveBeenCalledTimes(2);
		expect(metrics.histogram).toHaveBeenCalledWith('sessions.cap_scan.cache_hits', 1005);
		expect(metrics.histogram).toHaveBeenCalledWith('sessions.cap_scan.reads', 2);
	});

	it('bounds warm read latency by active records when retained GETs are slow', async () => {
		await Promise.all(Array.from({ length: 256 }, () => seed({ status: 'terminated' })));
		await seed();
		const get = bucket.get.bind(bucket);
		vi.spyOn(bucket, 'get').mockImplementation(async (path) => {
			await new Promise((resolve) => setTimeout(resolve, 10));
			return get(path);
		});
		vi.useFakeTimers();
		const cold = sessions.listActiveForUser(ACTOR);
		await vi.runAllTimersAsync();
		await cold;
		expect(metrics.histogram).toHaveBeenCalledWith(
			'sessions.cap_scan.read_ms',
			Math.ceil(257 / BUCKET_SCAN_CONCURRENCY) * 10,
		);
		metrics.histogram.mockClear();
		const warm = sessions.listActiveForUser(ACTOR);
		await vi.runAllTimersAsync();
		await warm;
		expect(metrics.histogram).toHaveBeenCalledWith('sessions.cap_scan.read_ms', 10);
		expect(metrics.histogram).toHaveBeenCalledWith('sessions.cap_scan.reads', 1);
	});

	it('rereads changed terminal records and applies current ownership and status', async () => {
		const record = await seed({ status: 'failed', user_id: uid('other') });
		expect(await sessions.listActiveForUser(ACTOR)).toEqual([]);
		const changed = { ...record, user_id: ACTOR, status: 'running' as const };
		await bucket.put(key(record), JSON.stringify(changed));
		expect(await sessions.listActiveForUser(ACTOR)).toEqual([changed]);
	});

	it('rereads terminal apps until sandbox reclamation is confirmed', async () => {
		const app = await seed({ mode: 'app', status: 'failed', sandbox_id: createSandboxId() });
		for (let i = 0; i < 2; i++) {
			expect(await sessions.listActiveForUser(ACTOR, 'project')).toEqual([app]);
		}
		const get = vi.spyOn(bucket, 'get');
		await bucket.put(key(app), JSON.stringify({ ...app, sandbox_reclaimed_at: app.started_at }));
		expect(await sessions.listActiveForUser(ACTOR, 'project')).toEqual([]);
		get.mockClear();
		expect(await sessions.listActiveForUser(ACTOR, 'project')).toEqual([]);
		expect(get).not.toHaveBeenCalled();
	});

	it('does not cache terminating sessions', async () => {
		const stopping = await seed({ status: 'terminating' });
		await sessions.listActiveForUser(ACTOR);
		const get = vi.spyOn(bucket, 'get');
		await sessions.listActiveForUser(ACTOR);
		expect(get).toHaveBeenCalledWith(key(stopping));
	});

	it('handles deletion and recreation of a cached key', async () => {
		const record = await seed({ status: 'terminated' });
		await sessions.listActiveForUser(ACTOR);
		await bucket.delete(key(record));
		expect(await sessions.listActiveForUser(ACTOR)).toEqual([]);
		const live = { ...record, status: 'running' as const };
		await bucket.put(key(record), JSON.stringify(live));
		expect(await sessions.listActiveForUser(ACTOR)).toEqual([live]);
	});

	it.each(['list', 'get'] as const)(
		'does not skip records when %s supplies no ETag',
		async (operation) => {
			const record = await seed({ status: 'terminated' });
			if (operation === 'list') {
				const list = bucket.list.bind(bucket);
				vi.spyOn(bucket, 'list').mockImplementation(async (options) => {
					const result = await list(options);
					return { ...result, objects: result.objects.map((obj) => ({ ...obj, etag: '' })) };
				});
			} else {
				const get = bucket.get.bind(bucket);
				vi.spyOn(bucket, 'get').mockImplementation(async (path) => {
					const body = await get(path);
					return body ? { ...body, etag: '' } : null;
				});
			}
			const get = vi.spyOn(bucket, 'get');
			await sessions.listActiveForUser(ACTOR);
			await sessions.listActiveForUser(ACTOR);
			expect(get).toHaveBeenCalledTimes(2);
			expect(get).toHaveBeenCalledWith(key(record));
		},
	);

	it('caches the body ETag when a write occurs between list and get', async () => {
		const record = await seed();
		const get = bucket.get.bind(bucket);
		const read = vi.spyOn(bucket, 'get').mockImplementationOnce(async (path) => {
			await bucket.put(path, JSON.stringify({ ...record, status: 'terminated' }));
			return get(path);
		});
		expect(await sessions.listActiveForUser(ACTOR)).toEqual([]);
		read.mockClear();
		expect(await sessions.listActiveForUser(ACTOR)).toEqual([]);
		expect(read).not.toHaveBeenCalled();
	});

	it('bounds retained cache entries', async () => {
		const records = await Promise.all(
			Array.from({ length: 4097 }, () => seed({ status: 'terminated' })),
		);
		records.sort((a, b) => key(a).localeCompare(key(b)));
		await sessions.listActiveForUser(ACTOR);
		await bucket.delete(records.slice(1, -1).map(key));
		const get = vi.spyOn(bucket, 'get');
		await sessions.listActiveForUser(ACTOR);
		expect(get.mock.calls).toEqual([[key(records[0])]]);
	});

	it('retains warm hits when the scan exceeds the cache capacity', async () => {
		await Promise.all(Array.from({ length: 8192 }, () => seed({ status: 'terminated' })));
		const get = vi.spyOn(bucket, 'get');
		expect(await sessions.listActiveForUser(ACTOR)).toEqual([]);
		expect(get).toHaveBeenCalledTimes(8192);
		for (let scan = 0; scan < 2; scan++) {
			get.mockClear();
			metrics.histogram.mockClear();
			expect(await sessions.listActiveForUser(ACTOR)).toEqual([]);
			expect(get).toHaveBeenCalledTimes(4096);
			expect(metrics.histogram).toHaveBeenCalledWith('sessions.cap_scan.cache_hits', 4096);
		}
	});

	it('drains in-flight reads after failure and reports their complete timings', async () => {
		await Promise.all(Array.from({ length: 80 }, () => seed()));
		const originalGet = bucket.get.bind(bucket);
		let reads = 0;
		let inFlight = 0;
		let maxInFlight = 0;
		const get = vi.spyOn(bucket, 'get').mockImplementation(async (path) => {
			const attempt = reads++;
			maxInFlight = Math.max(maxInFlight, ++inFlight);
			try {
				await new Promise((resolve) => setTimeout(resolve, attempt === 0 ? 1 : 20));
				if (attempt === 0) throw new Error('read unavailable');
				return await originalGet(path);
			} finally {
				inFlight--;
			}
		});
		vi.useFakeTimers();
		const rejected = expect(sessions.listActiveForUser(ACTOR)).rejects.toThrow('read unavailable');
		await vi.advanceTimersByTimeAsync(1);
		expect(get).toHaveBeenCalledTimes(BUCKET_SCAN_CONCURRENCY);
		expect(metrics.histogram).not.toHaveBeenCalled();
		await vi.runAllTimersAsync();
		await rejected;
		expect(inFlight).toBe(0);
		expect(maxInFlight).toBe(BUCKET_SCAN_CONCURRENCY);
		expect(get).toHaveBeenCalledTimes(BUCKET_SCAN_CONCURRENCY);
		expect(metrics.histogram).toHaveBeenCalledWith(
			'sessions.cap_scan.reads',
			BUCKET_SCAN_CONCURRENCY,
		);
		expect(metrics.histogram).toHaveBeenCalledWith('sessions.cap_scan.read_ms', 20);
		expect(metrics.histogram).toHaveBeenCalledWith('sessions.cap_scan.max_read_ms', 20);
	});

	it('propagates list and read failures with phase timings', async () => {
		const record = await seed();
		vi.spyOn(bucket, 'list').mockRejectedValueOnce(new Error('list unavailable'));
		await expect(sessions.listActiveForUser(ACTOR)).rejects.toThrow('list unavailable');
		expect(metrics.histogram).toHaveBeenCalledWith('sessions.cap_scan.list_ms', expect.any(Number));
		vi.spyOn(bucket, 'get').mockRejectedValueOnce(new Error('read unavailable'));
		await expect(sessions.listActiveForUser(ACTOR)).rejects.toThrow('read unavailable');
		expect(metrics.histogram).toHaveBeenCalledWith('sessions.cap_scan.read_ms', expect.any(Number));
		expect(await sessions.listActiveForUser(ACTOR)).toEqual([record]);
	});
});
