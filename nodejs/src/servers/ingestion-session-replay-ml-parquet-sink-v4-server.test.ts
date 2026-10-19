import { KAFKA_SESSION_REPLAY_ML_INDEX_V4 } from '~/common/config/kafka-topics'

import { buildV4SinkConsumerConfig } from './ingestion-session-replay-ml-parquet-sink-v4-server'
import { buildMlMirrorServerConfig } from './ml-mirror-server-config'

describe('v4 Parquet sink wiring', () => {
    it('reads the v4 index topic in its own group, storing offsets only after a flush', () => {
        const config = buildV4SinkConsumerConfig(buildMlMirrorServerConfig({}))
        expect(config).toMatchObject({
            topic: KAFKA_SESSION_REPLAY_ML_INDEX_V4,
            groupId: 'session-replay-ml-parquet-sink-v4',
            autoCommit: true,
            autoOffsetStore: false,
            callEachBatchWhenEmpty: true,
        })
    })

    it.each(['', 'session-replay-ml-parquet-sink'])('refuses the group %p', (group) => {
        const config = buildMlMirrorServerConfig({ AI_RESEARCH_REPLAY_V4_PARQUET_SINK_GROUP_ID: group })
        expect(() => buildV4SinkConsumerConfig(config)).toThrow('AI_RESEARCH_REPLAY_V4_PARQUET_SINK_GROUP_ID')
    })
})
