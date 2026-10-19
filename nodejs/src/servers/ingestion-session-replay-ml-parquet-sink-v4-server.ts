import { Message } from 'node-rdkafka'

import { KAFKA_SESSION_REPLAY_ML_INDEX_V4 } from '~/common/config/kafka-topics'
import { KafkaConsumer, KafkaConsumerConfig, parseKafkaHeaders } from '~/common/kafka/consumer/consumer-v1'
import { logger } from '~/common/utils/logger'
import { IndexBatcher } from '~/ingestion/pipelines/sessionreplay/ai-research/parquet-sink-v4/index-batcher'
import { INDEX_HEADER } from '~/ingestion/pipelines/sessionreplay/ai-research/parquet-sink-v4/index-message'
import { IndexParquetStore } from '~/ingestion/pipelines/sessionreplay/ai-research/parquet-sink-v4/index-parquet-store'
import {
    CaptureWatermark,
    CapturedData,
    capturedRecords,
    releasingOffsetStore,
} from '~/ingestion/pipelines/sessionreplay/shared/capture-watermark'
import { buildSessionRecordingS3Client } from '~/ingestion/pipelines/sessionreplay/shared/s3-client'

import { CleanupResources } from './base-server'
import { MlMirrorConsumerServer } from './ml-mirror-consumer-server'
import { IngestionSessionReplayMlMirrorServerConfig } from './ml-mirror-server-config'

/** A v4 sink pod in the v3 sink group would take v3 partitions and read none of their topic. */
function assertOwnGroup(config: IngestionSessionReplayMlMirrorServerConfig): void {
    const group = config.AI_RESEARCH_REPLAY_V4_PARQUET_SINK_GROUP_ID
    if (!group || group === config.SESSION_RECORDING_ML_PARQUET_SINK_GROUP_ID) {
        throw new Error('AI_RESEARCH_REPLAY_V4_PARQUET_SINK_GROUP_ID must be set and differ from the v3 sink group')
    }
}

/**
 * Manual offsets and callEachBatchWhenEmpty, as in the v3 sink: the batcher stores offsets only after their files are
 * written, and idle polls still reach the flush interval.
 */
export function buildV4SinkConsumerConfig(config: IngestionSessionReplayMlMirrorServerConfig): KafkaConsumerConfig {
    assertOwnGroup(config)
    return {
        topic: KAFKA_SESSION_REPLAY_ML_INDEX_V4,
        groupId: config.AI_RESEARCH_REPLAY_V4_PARQUET_SINK_GROUP_ID,
        autoCommit: true,
        autoOffsetStore: false,
        callEachBatchWhenEmpty: true,
    }
}

/** Block rows feed the event metadata watermark and image rows the inline image one, as in v3. */
function indexData(message: Message): CapturedData | undefined {
    const index = parseKafkaHeaders(message.headers)[INDEX_HEADER]
    return index === 'blocks' ? 'event_metadata' : index === 'images' ? 'inline_images' : undefined
}

export class IngestionSessionReplayMlParquetSinkV4Server extends MlMirrorConsumerServer {
    protected override readonly metricLabels: Record<string, string> = { ml_parquet_sink: 'v4' }

    protected async startServices(): Promise<void> {
        const consumerConfig = buildV4SinkConsumerConfig(this.config)
        const bucket = this.config.AI_RESEARCH_REPLAY_V4_RRWEB_BUCKET
        if (!bucket) {
            throw new Error('AI_RESEARCH_REPLAY_V4_RRWEB_BUCKET must be set for the v4 Parquet sink')
        }
        const rowGroupRows = this.config.AI_RESEARCH_REPLAY_V4_PARQUET_ROW_GROUP_ROWS
        if (!Number.isInteger(rowGroupRows) || rowGroupRows <= 0) {
            throw new Error('AI_RESEARCH_REPLAY_V4_PARQUET_ROW_GROUP_ROWS must be a positive integer')
        }
        const s3Client = buildSessionRecordingS3Client(this.config)
        if (!s3Client) {
            throw new Error('SESSION_RECORDING_V2_S3_* must be configured for the v4 Parquet sink')
        }

        const consumer = new KafkaConsumer(consumerConfig)
        const watermark = new CaptureWatermark('parquet_sink_v4')
        const batcher = new IndexBatcher(
            new IndexParquetStore(s3Client, bucket, rowGroupRows),
            releasingOffsetStore(consumer, watermark),
            {
                flushIntervalMs: this.config.AI_RESEARCH_REPLAY_V4_PARQUET_FLUSH_INTERVAL_MS,
                maxRows: this.config.AI_RESEARCH_REPLAY_V4_PARQUET_MAX_ROWS,
                maxBytes: this.config.AI_RESEARCH_REPLAY_V4_PARQUET_MAX_BYTES,
            },
            Date.now()
        )
        await consumer.connect(
            (messages) => {
                consumer.heartbeat()
                watermark.hold(capturedRecords(messages, indexData))
                return batcher.handleBatch(messages, Date.now())
            },
            (partitions) => {
                batcher.release(partitions)
                watermark.forget(partitions)
                return Promise.resolve()
            }
        )

        this.lifecycle.services.push({
            id: 'session-replay-ml-parquet-sink-v4',
            // Flush the buffer so a graceful restart does not read it again. A failed final flush replays on restart.
            onShutdown: async () => {
                try {
                    await batcher.flush(Date.now())
                } catch (error) {
                    logger.warn('🪶', 'ml_v4_parquet_sink_shutdown_flush_failed', { error: String(error) })
                }
                await consumer.disconnect()
            },
            healthcheck: () => consumer.isHealthy(),
        })
    }

    protected getCleanupResources(): CleanupResources {
        return { kafkaProducers: [], redisPools: [] }
    }
}
