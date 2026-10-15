import { teamMonthKeyExpiresAtMs } from '~/ingestion/pipelines/sessionreplay/ai-research/v4-keys/team-month-keys'
import { parseImageRef, parseV4UrlRef } from '~/ingestion/pipelines/sessionreplay/ml-mirror-image-scrub/content-ref'

import { explicitFreshnessLifetimeMs } from './configuration-policy'
import { HttpCacheMetadata } from './crawl-history'

export function urlHistoryExpiresAtMs(
    ref: string,
    nowMs: number,
    seenTtlSeconds: number,
    cache?: HttpCacheMetadata
): number {
    const month = parseImageRef(ref)?.sessionMonth
    if (month !== undefined) {
        // The stored image is write-once per team, month and URL, so a fetch before the month partition ends cannot change it, and HTTP freshness does not shorten this.
        const end = new Date(`${month}-01T00:00:00Z`)
        end.setUTCMonth(end.getUTCMonth() + 1)
        end.setUTCDate(end.getUTCDate() + 8)
        return end.getTime()
    }
    const minimumNextFetchAtMs = nowMs + seenTtlSeconds * 1000
    const explicitNextFetchAtMs = cache ? nowMs + explicitFreshnessLifetimeMs(cache, nowMs) : 0
    return Math.max(minimumNextFetchAtMs, explicitNextFetchAtMs)
}

/**
 * When the history entry of a reference may go. A v4 reference is fetched again as a global one is, and its entry
 * stays until the key row of its team month expires, because the lane refreshes the stored image from it.
 */
export function urlHistoryStorageExpiresAtMs(ref: string, nextFetchAtMs: number): number {
    const v4 = parseV4UrlRef(ref)
    return v4 ? teamMonthKeyExpiresAtMs(v4.keyMonth) : nextFetchAtMs
}

export function storedUrlHistoryCache(
    ref: string,
    cache: HttpCacheMetadata | undefined
): HttpCacheMetadata | undefined {
    const isMonthScoped = parseImageRef(ref)?.sessionMonth !== undefined
    const forbidsStoring = /(?:^|,)\s*no-store\s*(?:,|$)/i.test(cache?.cacheControl ?? '')
    return isMonthScoped && forbidsStoring ? undefined : cache
}
