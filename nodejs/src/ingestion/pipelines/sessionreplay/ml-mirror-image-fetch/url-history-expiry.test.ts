import { urlHistoryExpiresAtMs, urlHistoryStorageExpiresAtMs } from './url-history-expiry'

describe('urlHistoryExpiresAtMs', () => {
    const NOW_MS = Date.parse('2026-10-15T12:00:00Z')
    const TTL_SECONDS = 30 * 24 * 60 * 60
    const NEXT_FETCH_MS = NOW_MS + TTL_SECONDS * 1000

    it.each([
        // A month-scoped image is write-once for its month, so it is not fetched again before the partition ends.
        [
            'a v3 reference',
            'imageurl:v3:7:2026-10:aaaaaaaaaaaaaaaaaaaaaa',
            Date.parse('2026-11-09T00:00:00Z'),
            undefined,
        ],
        // A v4 reference is fetched again as a global one is, and its entry lives as long as its team month key row.
        [
            'a v4 reference',
            'imageurl:v4:7:2026-10:aaaaaaaaaaaaaaaaaaaaaa',
            NEXT_FETCH_MS,
            Date.parse('2027-03-01T00:00:00Z'),
        ],
        ['a global reference', 'imageurl:aaaaaaaaaaaaaaaaaaaaaa', NEXT_FETCH_MS, undefined],
    ])('expires the history of %s', (_name, ref, nextFetchAtMs, storageExpiresAtMs) => {
        expect(urlHistoryExpiresAtMs(ref, NOW_MS, TTL_SECONDS)).toBe(nextFetchAtMs)
        expect(urlHistoryStorageExpiresAtMs(ref, nextFetchAtMs)).toBe(storageExpiresAtMs ?? nextFetchAtMs)
    })
})
