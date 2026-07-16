import assert from 'node:assert/strict';
import { setTimeout as wait } from 'node:timers/promises';
import test from 'node:test';
import { KeyedQueue, TaskQueue } from '../src/utils/queues.js';

test('TaskQueue bounds concurrency and allows work with max_queued zero', async () => {
    const queue = new TaskQueue({ concurrency: 2, maximum: 0 });
    let active = 0;
    let maximum = 0;
    const task = () => queue.run(async () => {
        active++;
        maximum = Math.max(maximum, active);
        await wait(20);
        active--;
    });

    const first = task();
    const second = task();
    await assert.rejects(task(), { code: 'QUEUE_FULL' });
    await Promise.all([first, second]);
    assert.equal(maximum, 2);
});

test('TaskQueue times out queued work and rejects work after close', async () => {
    const queue = new TaskQueue({ concurrency: 1, maximum: 1, timeout: 10 });
    const active = queue.run(() => wait(40));
    await assert.rejects(queue.run(() => undefined), { code: 'QUEUE_TIMEOUT' });
    await active;
    queue.close();
    await assert.rejects(queue.run(() => undefined), { name: 'AbortError' });
});

test('TaskQueue preserves legacy throttle rate and interval options', async () => {
    const queue = new TaskQueue({
        concurrency: 3,
        throttle: { rate: 1, interval: 30 },
    });
    const starts = [];
    await Promise.all([0, 1, 2].map(() => queue.run(() => starts.push(Date.now()))));
    assert.ok(starts[1] - starts[0] >= 20, `First gap was ${starts[1] - starts[0]}ms.`);
    assert.ok(starts[2] - starts[1] >= 20, `Second gap was ${starts[2] - starts[1]}ms.`);
});

test('KeyedQueue serializes each URI without blocking independent URIs', async () => {
    const queue = new KeyedQueue();
    const events = [];
    const first = queue.run('/same', async () => {
        events.push('first:start');
        await wait(20);
        events.push('first:end');
    });
    const second = queue.run('/same', () => events.push('second'));
    const other = queue.run('/other', () => events.push('other'));
    await Promise.all([first, second, other]);
    assert.ok(events.indexOf('other') < events.indexOf('first:end'));
    assert.ok(events.indexOf('second') > events.indexOf('first:end'));
    await queue.idle();
    queue.close();
    await assert.rejects(queue.run('/same', () => undefined), { name: 'AbortError' });
});
