package no.nav.kafka.hot.crm

val devContext = env(config_DEPLOY_CLUSTER) == "dev-gcp"

val application =
    when (
        env(config_DEPLOY_APP)
    ) {
        "sf-payment-status" -> KafkaPosterApplication(modifier = ::extractStatusField)
        "sf-resource-number" -> KafkaPosterApplication()
        else -> throw RuntimeException("Attempted to deploy unknown app")
    }

fun main() = application.start()
