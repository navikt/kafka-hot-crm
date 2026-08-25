package no.nav.kafka.hot.crm.kafka

import org.apache.kafka.clients.consumer.KafkaConsumer

class InvestigateConsumerFactory : ConsumerFactory {
    override fun createConsumer(): KafkaConsumer<String, String?> = KafkaConsumer<String, String?>(propertiesInvestigate)
}
