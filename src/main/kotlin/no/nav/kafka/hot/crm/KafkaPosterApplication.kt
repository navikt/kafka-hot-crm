package no.nav.kafka.hot.crm

import io.prometheus.client.hotspot.DefaultExports
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import no.nav.kafka.hot.crm.db.PostgresDatabase
import no.nav.kafka.hot.crm.metrics.WorkSessionStatistics
import no.nav.kafka.hot.crm.nais.ShutdownHook
import no.nav.kafka.hot.crm.nais.naisAPI
import no.nav.kafka.hot.crm.poster.KafkaToSFPoster
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.http4k.server.Netty
import org.http4k.server.asServer
import kotlin.inc

/**
 * KafkaPosterApplication
 * This is the top level of the integration. Its function is to set up a server with the required
 * endpoints for the kubernetes environment
 * and create a work loop that alternatives between work sessions (i.e polling from kafka until we are in sync) and
 * an interruptable pause (configured with MS_BETWEEN_WORK).
 */
class KafkaPosterApplication(
    filter: ((ConsumerRecord<String, String?>) -> Boolean)? = null,
    modifier: ((ConsumerRecord<String, String?>) -> String?)? = null,
) {
    private val poster = KafkaToSFPoster(filter, modifier)

    private val msBetweenWork = env(config_MS_BETWEEN_WORK).toLong()

    private val log = KotlinLogging.logger { }

    private var runOnce = false

    private val useDbOffsetFallback = env(config_USE_DB_OFFSET_FALLBACK).toBoolean()

    var db: PostgresDatabase? = null

    fun start() {
        log.info {
            "Starting app ${env(
                config_DEPLOY_APP,
            )} - devContext $devContext - USE_DB_OFFSET_FALLBACK ${env(config_USE_DB_OFFSET_FALLBACK).toBoolean()}" +
                (if (env(config_FLAG_SEEK).toBoolean()) " - SEEK ${env(config_SEEK_OFFSET).toLong()}" else "") +
                (if (env(config_NUMBER_OF_SAMPLES).toInt() > 0) " - SAMPLE ${env(config_NUMBER_OF_SAMPLES)}" else "") +
                (if (env(config_FLAG_NO_POST).toBoolean()) " - NO_POST" else "") +
                (if (env(config_FLAG_ALT_ID).toBoolean()) " - ALT_ID" else "")
        }

        if (useDbOffsetFallback) {
            db = PostgresDatabase()
        }
        DefaultExports.initialize() // Instantiate Prometheus standard metrics
        naisAPI().asServer(Netty(8080)).start()

        while (!ShutdownHook.isActive()) {
            try {
                poster.runWorkSession()
                // RerunUtility.filterAndReport()
            } catch (e: Exception) {
                log.error { "A work session failed: \n${e.stackTraceToString()}" }
                WorkSessionStatistics.workSessionExceptionCounter.inc()
            }
            conditionalWait(msBetweenWork)
        }
    }

    /**
     * conditionalWait
     * Interruptable wait function
     */
    private fun conditionalWait(ms: Long) =
        runBlocking {
            log.debug { "Will wait $ms ms" }

            val waitJob =
                launch {
                    runCatching { delay(ms) }
                        .onSuccess { log.info { "waiting completed" } }
                        .onFailure { log.info { "waiting interrupted" } }
                }

            tailrec suspend fun loop(): Unit =
                when {
                    waitJob.isCompleted -> Unit
                    ShutdownHook.isActive() -> waitJob.cancel()
                    else -> {
                        delay(250L)
                        loop()
                    }
                }
            loop()
            waitJob.join()
        }
}
