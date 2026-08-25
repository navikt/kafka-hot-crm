package no.nav.kafka.hot.crm.resend

import mu.KotlinLogging
import no.nav.kafka.hot.crm.hasVergemaalEllerFremtidsfullmakt
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.io.File

object RerunUtility {
    private val log = KotlinLogging.logger { }

    const val POPULATE_CACHE: Boolean = false // Currently only active if also no_post set to true

    fun addToCache(records: Iterable<ConsumerRecord<String, String?>>) {
        records.forEach { cache[it.key()] = Pair(it.offset(), hasVergemaalEllerFremtidsfullmakt(it)) }
    }

    val cache: MutableMap<String, Pair<Long, Boolean>> = mutableMapOf()

    fun filterAndReport() {
        File("/tmp/offsetsToWithLatestVergeMal").writeText("")

        log.info {
            "Cache size ${cache.size}, after filter size ${cache.values.filter {
                if (it.second) File("/tmp/offsetsToWithLatestVergeMal").appendText(it.first.toString() + "\n")
                it.second
            }.size}}"
        }
    }
}
