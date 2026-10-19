/**
 * The columns of the two v4 index files, in file order, with parquetjs types. This table drives the row validator and
 * the Parquet writer, as `ml-mirror/block-metadata-columns.ts` does for v3. Kept free of the Parquet dependency so the
 * message-parsing path does not pull it in.
 */

export type V4Index = 'blocks' | 'images'

export type ParquetType = 'UTF8' | 'BOOLEAN' | 'INT32' | 'INT64'

export interface V4LeafColumn {
    name: string
    type: ParquetType
    /** Nullable column: it can be null or absent on a row. */
    optional?: boolean
}

/** A repeated group of required leaves; Parquet readers see it as a non-null list of structs. */
export interface V4RepeatedColumn {
    name: string
    repeated: true
    fields: readonly V4LeafColumn[]
}

export type V4Column = V4LeafColumn | V4RepeatedColumn

const required = (type: ParquetType, ...names: string[]): V4LeafColumn[] => names.map((name) => ({ name, type }))
const optional = (type: ParquetType, ...names: string[]): V4LeafColumn[] =>
    names.map((name) => ({ name, type, optional: true }))

export const MOMENT_COLUMNS: readonly V4LeafColumn[] = [
    ...required('UTF8', 'kind', 'window_id'),
    ...required('INT64', 'ts_ms'),
    ...required('INT32', 'event_index'),
]

export const IMAGE_INDEX_COLUMNS: readonly V4Column[] = [
    ...required('INT64', 'team_id'),
    ...required('UTF8', 'key_month', 'image_ref', 'pack_s3_key'),
    ...required('INT64', 'byte_start', 'byte_end'),
    ...required('UTF8', 'mime'),
    ...required('INT64', 'byte_size'),
]

export const BLOCK_INDEX_COLUMNS: readonly V4Column[] = [
    ...required('INT64', 'team_id'),
    ...required('UTF8', 'session_id', 'key_month', 'block_s3_key', 'topic'),
    ...required('INT32', 'partition'),
    ...required('INT64', 'start_offset', 'end_offset'),
    ...required('UTF8', 'flush_id'),
    ...required('INT64', 'flushed_at_ms', 'block_byte_start', 'block_byte_end'),
    ...optional('INT64', 'first_ts_ms', 'last_ts_ms'),
    ...required('INT64', 'event_count', 'byte_size'),
    ...optional('UTF8', 'snapshot_source', 'snapshot_library', 'host'),
    ...required(
        'INT32',
        'full_snapshots',
        'events_before_first_full_snapshot',
        'orphan_mutations',
        'dom_nodes',
        'remote_stylesheets',
        'inline_stylesheets',
        'iframes',
        'canvases',
        'videos',
        'shadow_roots'
    ),
    ...required('INT64', 'max_gap_ms', 'active_ms', 'dom_text_chars'),
    ...required('BOOLEAN', 'cold_start', 'node_cap_reached', 'touch_input'),
    ...required(
        'INT32',
        'clicks',
        'rage_clicks',
        'dead_clicks',
        'keypresses',
        'scrolls',
        'mouse_moves',
        'touches',
        'selections',
        'media_events',
        'viewport_resizes',
        'navigations',
        'console_errors',
        'console_warnings',
        'network_requests',
        'network_failures',
        'images_extracted',
        'images_inline',
        'image_urls'
    ),
    ...required('INT64', 'image_bytes'),
    ...optional('INT32', 'viewport_width', 'viewport_height'),
    ...optional('INT64', 'viewport_ts_ms'),
    { name: 'moments', repeated: true, fields: MOMENT_COLUMNS },
    ...optional('UTF8', 'urls'),
]

export const INDEX_COLUMNS: Record<V4Index, readonly V4Column[]> = {
    blocks: BLOCK_INDEX_COLUMNS,
    images: IMAGE_INDEX_COLUMNS,
}

export type V4IndexRow = Record<string, unknown>

const INT32_MIN = -(2 ** 31)
const INT32_MAX = 2 ** 31 - 1

// JSON numbers above 2^53 lose precision on parse, so an INT64 column takes only safe integers.
function isEncodableValue(type: ParquetType, value: unknown): boolean {
    switch (type) {
        case 'UTF8':
            return typeof value === 'string'
        case 'BOOLEAN':
            return typeof value === 'boolean'
        case 'INT32':
            return Number.isInteger(value) && (value as number) >= INT32_MIN && (value as number) <= INT32_MAX
        case 'INT64':
            return Number.isSafeInteger(value)
    }
}

function selectColumns(value: unknown, columns: readonly V4Column[]): V4IndexRow | null {
    if (typeof value !== 'object' || value === null || Array.isArray(value)) {
        return null
    }
    const source = value as Record<string, unknown>
    const row: V4IndexRow = {}
    for (const column of columns) {
        const field = source[column.name]
        if ('repeated' in column) {
            if (!Array.isArray(field)) {
                return null
            }
            const items: V4IndexRow[] = []
            for (const item of field) {
                const selected = selectColumns(item, column.fields)
                if (!selected) {
                    return null
                }
                items.push(selected)
            }
            row[column.name] = items
        } else if (field === null || field === undefined) {
            if (!column.optional) {
                return null
            }
            row[column.name] = null
        } else if (isEncodableValue(column.type, field)) {
            row[column.name] = field
        } else {
            return null
        }
    }
    return row
}

/**
 * Returns the row's index columns, or null when a column is missing or a value does not fit its type. Keys that are
 * not columns are left out, as the v3 sink does. A bad row must not reach the writer, which would fail the whole flush.
 */
export function selectIndexRow(index: V4Index, value: unknown): V4IndexRow | null {
    return selectColumns(value, INDEX_COLUMNS[index])
}
