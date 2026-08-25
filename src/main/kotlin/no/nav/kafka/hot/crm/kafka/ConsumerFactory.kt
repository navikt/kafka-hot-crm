package no.nav.kafka.hot.crm.kafka

import org.apache.kafka.clients.consumer.KafkaConsumer

interface ConsumerFactory {
    fun createConsumer(): KafkaConsumer<String, String?>
}
