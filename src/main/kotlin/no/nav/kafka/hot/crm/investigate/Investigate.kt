package no.nav.kafka.hot.crm.investigate

import no.nav.kafka.hot.crm.config_DEPLOY_APP
import no.nav.kafka.hot.crm.config_DEPLOY_CLUSTER
import no.nav.kafka.hot.crm.env
import no.nav.kafka.hot.crm.kafka.InvestigateConsumerFactory
import no.nav.kafka.hot.crm.metrics.WorkSessionStatistics
import no.nav.kafka.hot.crm.poster.KafkaToSFPoster
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.http4k.core.HttpHandler
import org.http4k.core.Response
import org.http4k.core.Status
import java.io.File
import kotlin.time.DurationUnit
import kotlin.time.toDuration

object Investigate {
    @Volatile
    var investigateInProgress = false

    @Volatile
    var timeSpentLastInvestigate = 0L

    var lastOffsetSearch = 0L

    val investigateHandler: HttpHandler = {
        if (investigateInProgress) {
            Response(Status.OK).body("Investigate in progress\n${report()}")
        } else {
            val ids = it.query("id")?.split(",")
            val offset = it.query("offset")?.toLong() ?: 0
            if (ids == null) {
                Response(Status.OK).body(
                    "Use query parameter id with csv of ids to search for. " +
                        "Optionally use query parameter offset to seek to position instead of from beginning\n" +
                        (
                            if ((env(config_DEPLOY_APP) == "sf-plis") && (env(config_DEPLOY_CLUSTER) == "prod-gcp")) {
                                "(Ref: sf-plis prod offset 72043224 corresponds with 2024-09-18 00:00)\n"
                            } else {
                                ""
                            }
                        ) +
                        report(),
                )
            } else {
                WorkSessionStatistics.investigateConsumedCounter.clear()
                WorkSessionStatistics.investigateHitCounter.clear()
                val startTime = System.currentTimeMillis()
                investigateInProgress = true
                lastOffsetSearch = offset
                Thread {
                    val investigatePoster =
                        KafkaToSFPoster(
                            filter = createInvestigateFilter(ids),
                            kafkaConsumerFactory = InvestigateConsumerFactory(),
                            flagSeek = true,
                            seekOffset = offset,
                            numberOfSamples = 0,
                            flagNoPost = true,
                            metricsActive = false,
                        )

                    try {
                        investigatePoster.runWorkSession()
                    } finally {
                        timeSpentLastInvestigate = System.currentTimeMillis() - startTime
                        investigateInProgress = false
                    }
                }.start()

                Response(Status.OK).body(
                    "Will trigger search for these: " + ids.joinToString(" ") +
                        (if (timeSpentLastInvestigate > 0) ", last finished took ${formatDuration(timeSpentLastInvestigate)}" else "") +
                        (if (offset > 0) ", from offset $offset" else ""),
                )
            }
        }
    }

    private fun createInvestigateFilter(ids: List<String>): ((ConsumerRecord<String, String?>) -> Boolean) =
        { record: ConsumerRecord<String, String?> ->
            // Check if the key of the record matches any ID in the list
            if (ids.contains(record.key())) {
                WorkSessionStatistics.investigateHitCounter.inc()
                File("/tmp/investigate-${record.key()}").appendText(record.toString() + "\n\n")
            }
            true // Always return true
        }

    private fun formatDuration(durationInMillis: Long): String {
        val duration = durationInMillis.toDuration(DurationUnit.MILLISECONDS)
        return "${duration.inWholeMinutes}m ${duration.inWholeSeconds % 60}s"
    }

    private fun report(): String =
        "Current investigate count: ${
            WorkSessionStatistics.investigateConsumedCounter.get().toInt()
        }, hits: ${
            WorkSessionStatistics.investigateHitCounter.get().toInt()
        }" + (if (timeSpentLastInvestigate > 0) ", last finished took ${formatDuration(timeSpentLastInvestigate)}" else "") +
            (if (lastOffsetSearch > 0) " from offset $lastOffsetSearch" else "")
}
