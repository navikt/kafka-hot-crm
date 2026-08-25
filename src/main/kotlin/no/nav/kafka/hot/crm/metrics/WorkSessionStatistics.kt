package no.nav.kafka.hot.crm.metrics

import io.prometheus.client.Gauge
import no.nav.kafka.hot.crm.metrics.Prometheus.registerCounter
import no.nav.kafka.hot.crm.metrics.Prometheus.registerGauge
import no.nav.kafka.hot.crm.metrics.Prometheus.registerLabelGauge
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * This class stores session statistics for logging and monitoring purposes.
 */
data class WorkSessionStatistics(
    /**
     * Statistics for a single work session
     */
    private var consumed: Int = 0,
    private var blockedByFilter: Int = 0,
    private var posted: Int = 0,
    private val consumedOffsets: OffsetPairs = OffsetPairs(), // To log what range of offsets per partition has been consumed
    private val postedOffsets: OffsetPairs = OffsetPairs(), // To log what range of offsets per partition has been posted
) {
    /**
     * Statistics for the lifetime of the app
     */
    companion object Metrics {
        private val consumedCounter = registerCounter("consumed")
        private val postedCounter = registerCounter("posted")
        private val postedTombstonesCounter = registerCounter("posted_tombstones")
        private val latestConsumedOffsetGauge = registerLabelGauge("latest_consumed_offset", "partition")
        private val latestPostedOffsetGauge = registerLabelGauge("latest_posted_offset", "partition")
        private val blockedByFilterGauge = registerGauge("blocked_by_filter")
        val workSessionsWithoutEventsCounter = registerCounter("work_sessions_without_events")
        val subsequentWorkSessionsWithEventsCounter = registerCounter("subsequent_work_sessions_with_events")
        val failedSalesforceCallCounter = registerCounter("failed_salesforce_call")
        val workSessionExceptionCounter = registerCounter("work_session_exception")
        val investigateConsumedCounter = registerCounter("investigateConsumed")
        val investigateHitCounter = registerCounter("investigateHit")
    }

    fun hasConsumed(): Boolean = consumed != 0

    fun updateConsumedStatistics(consumedRecords: Iterable<ConsumerRecord<String, String?>>) {
        val count = consumedRecords.count()
        consumed += count
        consumedCounter.inc(count.toDouble())
        consumedOffsets.update(consumedRecords, latestConsumedOffsetGauge)
    }

    fun incBlockedByFilter(value: Int) {
        blockedByFilter += value
        blockedByFilterGauge.inc(value.toDouble())
    }

    fun updatePostedStatistics(postedRecords: Iterable<ConsumerRecord<String, String?>>) {
        val count = postedRecords.count()
        posted += count
        postedCounter.inc(count.toDouble())
        postedTombstonesCounter.inc(postedRecords.count { it.value() == null }.toDouble())
        postedOffsets.update(postedRecords, latestPostedOffsetGauge)
    }

    data class OffsetPairs(
        val firstOffsetPerPartition: MutableMap<Int, Long> = mutableMapOf(),
        val lastOffsetPerPartition: MutableMap<Int, Long> = mutableMapOf(),
    ) {
        override fun toString(): String {
            if (firstOffsetPerPartition.isEmpty()) return "NONE"
            return firstOffsetPerPartition.keys.sorted().joinToString(",") {
                "$it:[${firstOffsetPerPartition[it]}-${lastOffsetPerPartition[it]}]"
            }
        }

        fun update(
            records: Iterable<ConsumerRecord<String, String?>>,
            latestOffsetGauge: Gauge,
        ) {
            records.forEach {
                if (!this.firstOffsetPerPartition.containsKey(it.partition())) {
                    this.firstOffsetPerPartition[it.partition()] = it.offset()
                }
                this.lastOffsetPerPartition[it.partition()] = it.offset()
            }
            this.lastOffsetPerPartition.forEach { entry ->
                latestOffsetGauge.labels(entry.key.toString()).set(entry.value.toDouble())
            }
        }
    }
}
