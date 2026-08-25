package no.nav.kafka.hot.crm.db

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import mu.KotlinLogging
import no.nav.kafka.hot.crm.application
import no.nav.kafka.hot.crm.config_DEPLOY_APP
import no.nav.kafka.hot.crm.devContext
import no.nav.kafka.hot.crm.env
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.upsert
import java.time.Instant

class PostgresDatabase {
    private val log = KotlinLogging.logger { }

    val appName = env(config_DEPLOY_APP).uppercase().replace('-', '_')
    val contextLabel = if (devContext) "DEV" else "PROD"

    private val dbJdbcUrl = env("NAIS_DATABASE_${appName}_${appName}_${contextLabel}_JDBC_URL")

    // Note: exposed Database connect prepares for connections but does not actually open connections
    // That is handled via transaction {} ensuring connections are opened and closed properly
    val database = Database.connect(HikariDataSource(hikariConfig()))

    private fun hikariConfig(): HikariConfig =
        HikariConfig().apply {
            jdbcUrl = dbJdbcUrl // "jdbc:postgresql://localhost:$dbPort/$dbName" // This is where the cloud db proxy is located in the pod
            driverClassName = "org.postgresql.Driver"
            minimumIdle = 1
            maxLifetime = 26000
            maximumPoolSize = 10
            connectionTimeout = 250
            idleTimeout = 10000
            isAutoCommit = false
            // Isolation level that ensure the same snapshot of db during one transaction:
            transactionIsolation = "TRANSACTION_REPEATABLE_READ"
        }

    fun createKafkaOffsetTable(dropFirst: Boolean = false) {
        transaction {
            if (dropFirst) {
                log.info { "Dropping table $KAFKA_OFFSETS" }
                val dropStatement =
                    TransactionManager.current().connection.prepareStatement("DROP TABLE $KAFKA_OFFSETS", false)
                dropStatement.executeUpdate()
                log.info { "Drop performed" }
            }

            log.info { "Creating table $KAFKA_OFFSETS" }
            SchemaUtils.create(KafkaOffsets)
        }
    }

    fun saveOffset(
        topic: String,
        partition: Int,
        offset: Long,
    ) {
        transaction {
            KafkaOffsets.upsert(
                KafkaOffsets.topic,
                KafkaOffsets.partition,
            ) {
                it[KafkaOffsets.topic] = topic
                it[KafkaOffsets.partition] = partition
                it[KafkaOffsets.offset] = offset
                it[updatedAt] = Instant.now()
            }
        }
    }

    fun loadOffset(
        topic: String,
        partition: Int,
    ): Long? =
        transaction {
            KafkaOffsets
                .selectAll()
                .where {
                    (KafkaOffsets.topic eq topic) and
                        (KafkaOffsets.partition eq partition)
                }.map { it[KafkaOffsets.offset] }
                .singleOrNull()
        }

    fun loadOffsets(topicName: String): Map<Int, Long> =
        transaction {
            KafkaOffsets
                .selectAll()
                .where { KafkaOffsets.topic eq topicName }
                .associate { row ->
                    row[KafkaOffsets.partition] to row[KafkaOffsets.offset]
                }
        }
}
