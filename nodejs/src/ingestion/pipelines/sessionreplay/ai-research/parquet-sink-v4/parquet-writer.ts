/** Encodes v4 index rows into a Snappy-compressed Parquet buffer, with the types of the COLUMNS table. */
import { ParquetSchema } from '@dsnp/parquetjs'

import { parquetRecordsToBuffer } from '~/ingestion/pipelines/sessionreplay/shared/parquet'

import { INDEX_COLUMNS, V4Column, V4Index, V4IndexRow } from './index-columns'

type SchemaDef = ConstructorParameters<typeof ParquetSchema>[0]

// Snappy on every column: compression is a per-field option in parquetjs, not a writer option.
function schemaDef(columns: readonly V4Column[]): SchemaDef {
    const def: Record<string, unknown> = {}
    for (const column of columns) {
        if ('repeated' in column) {
            def[column.name] = { repeated: true, fields: schemaDef(column.fields) }
        } else {
            def[column.name] = { type: column.type, compression: 'SNAPPY', ...(column.optional && { optional: true }) }
        }
    }
    return def as SchemaDef
}

const SCHEMAS: Record<V4Index, ParquetSchema> = {
    blocks: new ParquetSchema(schemaDef(INDEX_COLUMNS.blocks)),
    images: new ParquetSchema(schemaDef(INDEX_COLUMNS.images)),
}

function toRecord(row: V4IndexRow, columns: readonly V4Column[]): Record<string, unknown> {
    const record: Record<string, unknown> = {}
    for (const column of columns) {
        const value = row[column.name]
        if ('repeated' in column) {
            record[column.name] = (value as V4IndexRow[]).map((item) => toRecord(item, column.fields))
        } else if (value === null || value === undefined) {
            record[column.name] = null
        } else if (column.type === 'INT64') {
            record[column.name] = BigInt(value as number)
        } else {
            record[column.name] = value
        }
    }
    return record
}

export function indexRowsToParquet(index: V4Index, rows: readonly V4IndexRow[], rowGroupSize: number): Promise<Buffer> {
    const columns = INDEX_COLUMNS[index]
    return parquetRecordsToBuffer(
        SCHEMAS[index],
        rows.map((row) => toRecord(row, columns)),
        { rowGroupSize }
    )
}
