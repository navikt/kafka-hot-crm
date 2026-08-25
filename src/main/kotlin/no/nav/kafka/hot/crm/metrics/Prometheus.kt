package no.nav.kafka.hot.crm.metrics

import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Counter
import io.prometheus.client.Gauge
import io.prometheus.client.exporter.common.TextFormat
import java.io.StringWriter

object Prometheus {
    val metricsAsText: String get() {
        val str = StringWriter()
        TextFormat.write004(str, CollectorRegistry.defaultRegistry.metricFamilySamples())
        return str.toString()
    }

    fun registerGauge(name: String) =
        Gauge
            .build()
            .name(name)
            .help(name)
            .register()

    fun registerLabelGauge(
        name: String,
        label: String,
    ) = Gauge
        .build()
        .name(name)
        .help(name)
        .labelNames(label)
        .register()

    fun registerCounter(name: String): Counter =
        Counter
            .build()
            .name(name)
            .help(name)
            .register()

    fun registerLabelCounter(
        name: String,
        label: String,
    ): Counter =
        Counter
            .build()
            .name(name)
            .help(name)
            .labelNames(label)
            .register()
}
