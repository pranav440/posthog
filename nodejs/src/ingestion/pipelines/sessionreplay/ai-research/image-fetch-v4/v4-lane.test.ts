import { GetItemCommand } from '@aws-sdk/client-dynamodb'
import { CopyObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3'
import { Message } from 'node-rdkafka'
import { createDecipheriv, generateKeyPairSync } from 'node:crypto'

import { KAFKA_SESSION_REPLAY_IMAGE_FETCH_V4_RETRY_1M } from '~/common/config/kafka-topics'
import { KafkaProducer } from '~/common/kafka/producer'
import { parseJSON } from '~/common/utils/json-parse'
import { StreamedResponse, fetchStreamed } from '~/common/utils/request'
import { TeamMonthKeys } from '~/ingestion/pipelines/sessionreplay/ai-research/v4-keys/team-month-keys'
import type { FetchCandidate } from '~/ingestion/pipelines/sessionreplay/ml-mirror-image-fetch/collected-urls-record'
import {
    CrawlHistoryItem,
    CrawlHistoryStore,
    UrlCrawlHistoryItem,
    configurationCacheKey,
} from '~/ingestion/pipelines/sessionreplay/ml-mirror-image-fetch/crawl-history'
import { FetchRunner } from '~/ingestion/pipelines/sessionreplay/ml-mirror-image-fetch/fetch-runner'
import { ImageFetchTopHogMetrics } from '~/ingestion/pipelines/sessionreplay/ml-mirror-image-fetch/tophog-metrics'
import { UrlFetchConsumer } from '~/ingestion/pipelines/sessionreplay/ml-mirror-image-fetch/url-fetch-consumer'
import vector from '~/ingestion/pipelines/sessionreplay/ml-mirror/keys/encryption-vector-v4.json'
import {
    buildFetchRunner,
    buildFrontierPublisher,
    v3HostLimits,
} from '~/servers/ingestion-session-replay-ml-image-fetch-server'
import { buildMlMirrorServerConfig } from '~/servers/ingestion-session-replay-ml-mirror-server'
import { createRecordingTopHog } from '~/tests/helpers/tophog'

import { V4JobAdmission } from './job-admission'
import { V4_IMAGE_FETCH_LANE, v4HostLimits } from './lane'
import { RawImageStore } from './raw-image-store'
import { SharedImageFetcher } from './shared-image-fetcher'

jest.mock('~/common/utils/request', () => ({
    ...jest.requireActual('~/common/utils/request'),
    fetchStreamed: jest.fn(),
}))

const fetchStreamedMock = fetchStreamed as jest.MockedFunction<typeof fetchStreamed>

const NOW_MS = Date.parse('2026-10-15T12:00:00Z')
const PNG = Buffer.concat([Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]), Buffer.from('png body')])
const TEAM_KEYS: Record<string, Buffer> = {
    'team:7': Buffer.from(vector.teamMonthKey, 'base64'),
    'team:8': Buffer.alloc(32, 8),
}
const OTHER_HASH = 'B'.repeat(22)
const DAY_MS = 24 * 60 * 60 * 1000
const IMAGE_OBJECT_KEY = `images/url/month=${vector.identity.keyMonth}/7/${vector.url.ref.split(':').pop()}`

function respond(status: number, headers: Record<string, string> = {}, bytes = Buffer.alloc(0)): StreamedResponse {
    return {
        status,
        headers,
        headerLines: Object.entries(headers).map(([name, value]) => ({ name, value })),
        read: jest.fn().mockResolvedValue({ bytes, overLimit: false }),
        discard: jest.fn(),
    }
}

/** A record in the shape `replay-recorder-v4/src/urls.rs` produces: keyed by registrable domain, with no headers. */
function recorderRecord(jobs: { ref: string; url: string }[]): Message {
    const value = Buffer.from(
        JSON.stringify({
            v: 2,
            jobs: jobs.map(({ ref, url }) => ({
                sessionId: vector.identity.sessionId,
                originalRef: ref,
                currentUrl: url,
                remainingHops: 10,
                notBeforeMs: 0,
                firstSeenAtMs: NOW_MS,
                fetchCount: 0,
                republishCount: 0,
                lastRepublishReason: null,
            })),
        })
    )
    return { value, key: Buffer.from('example.com'), size: value.length, topic: 'v4', partition: 0, offset: 0 }
}

function openFrame(key: Buffer, frame: Buffer): { context: Record<string, unknown>; body: Buffer } {
    const length = frame.readUInt16BE(6)
    const aad = frame.subarray(8, 8 + length)
    const nonce = frame.subarray(8 + length, 20 + length)
    const sealed = frame.subarray(20 + length)
    const decipher = createDecipheriv('aes-256-gcm', key, nonce, { authTagLength: 16 })
    decipher.setAAD(aad)
    decipher.setAuthTag(sealed.subarray(sealed.length - 16))
    const body = Buffer.concat([decipher.update(sealed.subarray(0, sealed.length - 16)), decipher.final()])
    return { context: parseJSON(aad.toString()).context, body }
}

/** The origin's robots.txt and tdmrep.json are known for a year, so the jobs do not wait on one configuration fetch. */
class MemoryCrawlHistory implements CrawlHistoryStore {
    public readonly items = new Map<string, CrawlHistoryItem>(
        (['robots', 'tdmrep'] as const).map((kind) => {
            const origin = new URL(vector.url.url).origin
            const nowMs = Date.now()
            const item: CrawlHistoryItem = {
                kind,
                key: configurationCacheKey(origin, kind),
                origin,
                status: 'absent',
                fetchedAtMs: nowMs,
                refreshAtMs: nowMs + 365 * DAY_MS,
                freshUntilMs: nowMs + 365 * DAY_MS,
                retryAtMs: nowMs,
                storageExpiresAtMs: nowMs + 365 * DAY_MS,
            }
            return [item.key, item]
        })
    )
    read(keys: string[]): Promise<Map<string, CrawlHistoryItem>> {
        return Promise.resolve(
            new Map(keys.flatMap((key) => (this.items.has(key) ? [[key, this.items.get(key)!]] : [])))
        )
    }
    write(items: CrawlHistoryItem[]): Promise<void> {
        items.forEach((item) => this.items.set(item.key, item))
        return Promise.resolve()
    }
}

/** `keyReadsHang` makes every key row read wait until its caller gives up on it. */
function lane(rows: Record<string, Record<string, any> | undefined>, keyReadsHang = false, keyBudgetMs?: number) {
    const config = buildMlMirrorServerConfig({
        WEB_BOT_AUTH_PRIVATE_KEYS: generateKeyPairSync('ed25519')
            .privateKey.export({ type: 'pkcs8', format: 'pem' })
            .toString(),
    })
    const producer = { produce: jest.fn(() => Promise.resolve()) } as unknown as KafkaProducer
    const publisher = buildFrontierPublisher(producer, 10, V4_IMAGE_FETCH_LANE)
    const dynamo = {
        send: jest.fn((command: GetItemCommand, options?: any) => {
            if (keyReadsHang) {
                return new Promise<never>((_resolve, reject) =>
                    options?.abortSignal?.addEventListener('abort', () => reject(options.abortSignal.reason))
                )
            }
            const id = `${command.input.Key!.pk.S}/${command.input.Key!.sk.S}`
            return Promise.resolve({ Item: rows[id] })
        }),
    }
    const kms = {
        send: jest.fn((command: any) =>
            Promise.resolve({ Plaintext: TEAM_KEYS[`team:${command.input.EncryptionContext.team_id}`] })
        ),
    }
    const keys = new TeamMonthKeys(dynamo, kms, 'keys', 'arn')
    const puts: PutObjectCommand[] = []
    const copies: CopyObjectCommand[] = []
    const s3 = {
        send: jest.fn((command: PutObjectCommand | CopyObjectCommand) => {
            if (command instanceof CopyObjectCommand) {
                copies.push(command)
            } else {
                puts.push(command)
            }
            return Promise.resolve({})
        }),
    }
    const store = new RawImageStore(keys, s3 as any, 'images-v4', 10)
    const topHog = new ImageFetchTopHogMetrics(createRecordingTopHog().registry)
    const runner = buildFetchRunner(
        config,
        publisher,
        topHog,
        store,
        v4HostLimits(v3HostLimits(config), config.AI_RESEARCH_REPLAY_V4_IMAGE_FETCH_HOST_BUDGET_SHARE),
        (fetcher) => new SharedImageFetcher(fetcher)
    )
    const admission = new V4JobAdmission(keys, keyBudgetMs)
    const deadLetters = { park: jest.fn(() => Promise.resolve()) }
    const history = new MemoryCrawlHistory()
    const consumer = new UrlFetchConsumer(
        history,
        publisher,
        {
            seenTtlSeconds: config.AI_RESEARCH_IMAGE_FETCH_CRAWL_HISTORY_TTL_SECONDS,
            dryRun: false,
            acceptsRef: V4_IMAGE_FETCH_LANE.acceptsRef,
            admitCandidates: (candidates, nowMs) => admission.admit(candidates, nowMs),
            refreshingSink: store,
        },
        runner,
        deadLetters,
        topHog
    )
    return { consumer, runner, puts, copies, history, deadLetters, producer, dynamo }
}

const row = (team: number, month = vector.identity.keyMonth, extra: Record<string, any> = {}) => ({
    [`team:${team}/month:${month}`]: {
        pk: { S: `team:${team}` },
        sk: { S: `month:${month}` },
        wrapped_key: { B: Buffer.from('w') },
        ...extra,
    },
})

describe('v4 image fetch lane', () => {
    const runners: FetchRunner[] = []

    beforeEach(() => {
        fetchStreamedMock.mockReset()
        fetchStreamedMock.mockImplementation((url) =>
            Promise.resolve(
                new URL(url).pathname.endsWith('.png')
                    ? respond(200, { 'content-type': 'image/png' }, PNG)
                    : respond(404)
            )
        )
    })

    afterEach(async () => {
        await Promise.all(runners.splice(0).map((runner) => runner.close()))
    })

    it("stores a recorder record's image under each team's month key, with one request for a URL two teams share", async () => {
        const harness = lane({ ...row(7), ...row(8) })
        runners.push(harness.runner)
        const otherTeamRef = `imageurl:v4:8:${vector.identity.keyMonth}:${OTHER_HASH}`

        await harness.consumer.handleBatch(
            [
                recorderRecord([
                    { ref: vector.url.ref, url: vector.url.url },
                    { ref: otherTeamRef, url: vector.url.url },
                ]),
            ],
            NOW_MS
        )

        expect(harness.deadLetters.park).not.toHaveBeenCalled()
        expect(fetchStreamedMock.mock.calls.filter(([url]) => url === vector.url.url)).toHaveLength(1)
        const stored = new Map(harness.puts.map((put) => [put.input.Key, put.input.Body as Buffer]))
        expect([...stored.keys()].sort()).toEqual([
            IMAGE_OBJECT_KEY,
            `images/url/month=${vector.identity.keyMonth}/8/${OTHER_HASH}`,
        ])
        for (const [team, ref] of [
            [7, vector.url.ref],
            [8, otherTeamRef],
        ] as const) {
            const objectKey = `images/url/month=${vector.identity.keyMonth}/${team}/${ref.split(':').pop()}`
            const { context, body } = openFrame(TEAM_KEYS[`team:${team}`], stored.get(objectKey)!)
            expect(body).toEqual(PNG)
            expect(context).toEqual({
                teamId: team,
                keyMonth: vector.identity.keyMonth,
                kind: 'image-url',
                codec: 'none',
                ref,
            })
        }
    })

    it.each([
        // The row of 2026-05 expired on 2026-10-01, and DynamoDB may not have removed it yet.
        ['a month whose key expired', row(7, '2026-05'), `imageurl:v4:7:2026-05:${OTHER_HASH}`],
        [
            'a tombstoned team month',
            row(7, vector.identity.keyMonth, { deleted: { BOOL: true }, wrapped_key: undefined }),
            vector.url.ref,
        ],
        ['a team month without a key row', {}, vector.url.ref],
    ])('drops a job for %s before any request', async (_name, rows, ref) => {
        const harness = lane(rows)
        runners.push(harness.runner)

        await harness.consumer.handleBatch([recorderRecord([{ ref, url: vector.url.url }])], NOW_MS)

        expect(fetchStreamedMock).not.toHaveBeenCalled()
        expect(harness.puts).toHaveLength(0)
        expect(harness.deadLetters.park).not.toHaveBeenCalled()
    })

    it('sends a job whose key was not read within the batch key budget to the first delay tier, with no request', async () => {
        const harness = lane(row(7), true, 50)
        runners.push(harness.runner)

        await harness.consumer.handleBatch([recorderRecord([{ ref: vector.url.ref, url: vector.url.url }])], NOW_MS)

        expect(fetchStreamedMock).not.toHaveBeenCalled()
        expect(harness.deadLetters.park).not.toHaveBeenCalled()
        const produced = (harness.producer.produce as jest.Mock).mock.calls.map(([record]) => record.topic)
        expect(produced).toEqual([KAFKA_SESSION_REPLAY_IMAGE_FETCH_V4_RETRY_1M])
        // The abandoned read was stopped, not left to run for its own budget.
        const [, readOptions] = harness.dynamo.send.mock.calls[0] as unknown as [unknown, { abortSignal: AbortSignal }]
        expect(readOptions.abortSignal.aborted).toBe(true)
    })

    describe('with a reference seen again later in its month', () => {
        const imageRequests = (): number =>
            fetchStreamedMock.mock.calls.filter(([url]) => url === vector.url.url).length
        const urlHistory = (harness: ReturnType<typeof lane>): UrlCrawlHistoryItem =>
            harness.history.items.get(vector.url.ref) as UrlCrawlHistoryItem
        const seeAt = async (harness: ReturnType<typeof lane>, nowMs: number): Promise<void> => {
            jest.setSystemTime(nowMs)
            await harness.consumer.handleBatch([recorderRecord([{ ref: vector.url.ref, url: vector.url.url }])], nowMs)
        }

        let harness: ReturnType<typeof lane>

        beforeEach(async () => {
            // Only the clock is fake: the fetch pass still runs on real timers.
            jest.useFakeTimers({
                now: NOW_MS,
                doNotFake: [
                    'nextTick',
                    'queueMicrotask',
                    'setImmediate',
                    'setTimeout',
                    'clearTimeout',
                    'setInterval',
                    'clearInterval',
                    'performance',
                    'hrtime',
                ],
            })
            fetchStreamedMock.mockImplementation((url, options) =>
                Promise.resolve(
                    url !== vector.url.url
                        ? respond(404)
                        : (options?.headers as Record<string, string> | undefined)?.['if-none-match'] === '"v1"'
                          ? respond(304, { etag: '"v1"' })
                          : respond(200, { 'content-type': 'image/png', etag: '"v1"' }, PNG)
                )
            )
            harness = lane(row(7))
            runners.push(harness.runner)
            await seeAt(harness, NOW_MS)
            expect([imageRequests(), harness.puts.length]).toEqual([1, 1])
        })

        afterEach(() => {
            jest.useRealTimers()
        })

        it('keeps its history until the key row of its team month expires', () => {
            expect(urlHistory(harness)).toMatchObject({
                storedAtMs: NOW_MS,
                storageExpiresAtMs: Date.parse('2027-03-01T00:00:00Z'),
            })
        })

        it('drops a job inside the refresh interval with no request and no write', async () => {
            await seeAt(harness, NOW_MS + 6 * DAY_MS)

            expect([imageRequests(), harness.puts.length, harness.copies.length]).toEqual([1, 1, 0])
        })

        it('refreshes the stored image by a copy onto itself after 7 days, with no origin request', async () => {
            await seeAt(harness, NOW_MS + 7 * DAY_MS)

            expect([imageRequests(), harness.puts.length]).toEqual([1, 1])
            expect(harness.copies.map((copy) => copy.input.Key)).toEqual([IMAGE_OBJECT_KEY])
            expect(urlHistory(harness).storedAtMs).toBe(NOW_MS + 7 * DAY_MS)

            await seeAt(harness, NOW_MS + 8 * DAY_MS)
            expect(harness.copies).toHaveLength(1)
        })

        it.each([
            ['refreshes the image by a copy when the origin answers 304', 304, 0, 1],
            ['overwrites the image when the origin answers 200', 200, 1, 0],
        ])('revalidates after the v3 refetch interval and %s', async (_name, status, puts, copies) => {
            if (status === 200) {
                fetchStreamedMock.mockImplementation(() =>
                    Promise.resolve(respond(200, { 'content-type': 'image/png', etag: '"v2"' }, PNG))
                )
            }
            const refetchAtMs = urlHistory(harness).nextFetchAtMs
            expect(refetchAtMs).toBe(NOW_MS + 30 * DAY_MS)
            // A fresh lane reads the history, so no fetch the first lane shared in memory serves this job.
            const later = lane(row(7))
            runners.push(later.runner)
            later.history.items.set(vector.url.ref, urlHistory(harness))

            await seeAt(later, refetchAtMs)

            const requests = fetchStreamedMock.mock.calls.filter(([url]) => url === vector.url.url)
            expect(requests).toHaveLength(2)
            expect((requests[1][1]?.headers as Record<string, string>)['if-none-match']).toBe('"v1"')
            expect([later.puts.length, later.copies.length]).toEqual([puts, copies])
            expect([...later.puts, ...later.copies].map((write) => write.input.Key)).toEqual([IMAGE_OBJECT_KEY])
            expect(urlHistory(later)).toMatchObject({
                storedAtMs: refetchAtMs,
                nextFetchAtMs: refetchAtMs + 30 * DAY_MS,
            })
        })
    })

    describe('with many slow key lookups', () => {
        beforeEach(() => {
            jest.useFakeTimers({ now: NOW_MS })
        })

        afterEach(() => {
            jest.useRealTimers()
        })

        it('finishes admission within the batch key budget, runs lookups bounded, and defers the jobs it did not reach', async () => {
            const LOOKUP_MS = 16_000
            let running = 0
            let mostRunning = 0
            let started = 0
            const keys = {
                get: async () => {
                    started++
                    running++
                    mostRunning = Math.max(mostRunning, running)
                    await new Promise((resolve) => setTimeout(resolve, LOOKUP_MS))
                    running--
                    return { status: 'ok' as const, key: Buffer.alloc(32) }
                },
            }
            const candidates = Array.from(
                { length: 256 },
                (_, team) =>
                    ({
                        originalRef: `imageurl:v4:${team + 1}:${vector.identity.keyMonth}:${OTHER_HASH}`,
                        notBeforeMs: 0,
                    }) as FetchCandidate
            )
            let admitted: FetchCandidate[] | undefined
            void new V4JobAdmission(keys, 60_000).admit(candidates, NOW_MS).then((result) => {
                admitted = result
            })

            await jest.advanceTimersByTimeAsync(60_000)

            expect(admitted).toHaveLength(256)
            expect(mostRunning).toBeLessThanOrEqual(16)
            // Lookups start at 0s, 16s, 32s and 48s, and only the first three rounds finish within 60s.
            const ready = admitted!.filter((candidate) => candidate.notBeforeMs === 0)
            expect(ready).toHaveLength(48)
            const deferred = admitted!.filter((candidate) => candidate.notBeforeMs > 0)
            expect(deferred).toHaveLength(208)
            expect(deferred.every((candidate) => candidate.notBeforeMs === NOW_MS + 60_000)).toBe(true)
            expect(deferred.every((candidate) => candidate.lastBlockReason === 'pass_deadline')).toBe(true)

            // A lookup still queued at the deadline never starts.
            await jest.advanceTimersByTimeAsync(60_000)
            expect(started).toBe(64)
        })
    })
})
