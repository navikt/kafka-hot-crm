package no.nav.kafka.hot.crm.db

import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.javatime.timestamp

const val KAFKA_OFFSETS = "kafka_offsets"

object KafkaOffsets : Table(KAFKA_OFFSETS) {
    val topic = varchar("topic", 200)
    val partition = integer("partition")

    val offset = long("offset")

    val updatedAt = timestamp("updated_at")

    override val primaryKey =
        PrimaryKey(topic, partition)
}
