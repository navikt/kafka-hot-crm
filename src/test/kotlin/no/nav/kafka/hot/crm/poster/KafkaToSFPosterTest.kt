@file:Suppress("ktlint:standard:max-line-length")

package no.nav.kafka.hot.crm.poster

import com.google.gson.Gson
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.nav.kafka.hot.crm.kafka.KafkaConsumerFactory
import no.nav.kafka.hot.crm.readResourceFile
import no.nav.kafka.hot.crm.salesforce.KafkaMessage
import no.nav.kafka.hot.crm.salesforce.SFsObjectStatus
import no.nav.kafka.hot.crm.salesforce.SalesforceClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.http4k.core.Response
import org.http4k.core.Status
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.Base64

class KafkaToSFPosterTest {
    private val exampleWithSalesforceTagRecord = readResourceFile("/exampleWithSalesforceTag.json").asRecordValue()
    private val exampleWithSalesforceTagWithOffset1Record = readResourceFile("/exampleWithSalesforceTag.json").asRecordValue(2L)

    private fun String?.asRecordValue(offset: Long = 1L) = ConsumerRecord("topic", 0, offset, "key", this)

    // Helper function to create an instance of ConsumerRecords from list of consumer records
    private fun List<ConsumerRecord<String, String?>>.toConsumerRecords(): ConsumerRecords<String, String?> =
        ConsumerRecords<String, String?>(mapOf(TopicPartition("topic", 0) to this))

    private val gson = Gson()

    private val sfClientMock = mockk<SalesforceClient>()
    private val kafkaConsumerFactoryMock = mockk<KafkaConsumerFactory>()
    private val kafkaConsumerMock = mockk<KafkaConsumer<String, String?>>()
    private val partitionInfoMock = mockk<PartitionInfo>()

    val filterMock: (ConsumerRecord<String, String?>) -> Boolean = mockk()

    // To be tested:
    lateinit var kafkaToSFPoster: KafkaToSFPoster

    // Parameters to be altered by test cases
    var filter: ((ConsumerRecord<String, String?>) -> Boolean)? = null
    var modifier: ((ConsumerRecord<String, String?>) -> String?)? = null

    @BeforeEach
    fun beforeEach() {
        setUp()
    }

    private fun setUp(
        flagSeek: Boolean = false,
        numberOfSamples: Int = 0,
        flagNoPost: Boolean = false,
    ) {
        every { kafkaConsumerFactoryMock.createConsumer() } returns kafkaConsumerMock
        every { kafkaConsumerMock.partitionsFor(any()) } returns listOf(partitionInfoMock)
        every { partitionInfoMock.topic() } returns "topic"
        every { partitionInfoMock.partition() } returns 0
        every { kafkaConsumerMock.assign(any()) } returns Unit

        every { sfClientMock.postRecords(any()) } returns Response(Status.OK).body("[${gson.toJson(SFsObjectStatus("id", true))}]")
        every { kafkaConsumerMock.commitSync() } returns Unit
        every { kafkaConsumerMock.close() } returns Unit

        kafkaToSFPoster =
            KafkaToSFPoster(
                filter = filter,
                modifier = modifier,
                sfClient = sfClientMock,
                kafkaConsumerFactory = kafkaConsumerFactoryMock,
                kafkaTopic = "topic",
                kafkaPollDuration = 10L,
                flagSeek = flagSeek,
                seekOffset = 0L,
                numberOfSamples = numberOfSamples,
                flagNoPost = flagNoPost,
            )
    }

    @AfterEach
    fun tearDown() {
        filter = null
        modifier = null
    }

    @Test
    fun `Two polls - when consumer pulls one record on two subsequent poll calls and none on a third call that work session should result in two post calls to salesforce`() {
        // Setup mock responses to poll:
        val pollResponse1 = listOf(exampleWithSalesforceTagRecord).toConsumerRecords() // Result of first poll call - one record
        val pollResponse2 = listOf(exampleWithSalesforceTagWithOffset1Record).toConsumerRecords() // Result of second poll call - one record
        val pollResponse3 = ConsumerRecords<String, String?>(mapOf()) // Result of third poll call - no records

        every { kafkaConsumerMock.poll(any<Duration>()) } returnsMany listOf(pollResponse1, pollResponse2, pollResponse3)

        kafkaToSFPoster.runWorkSession()

        verify(exactly = 2) {
            sfClientMock.postRecords(
                setOf(
                    KafkaMessage(
                        CRM_Topic__c = "topic",
                        CRM_Key__c = "key",
                        CRM_Value__c = Base64.getEncoder().encodeToString(readResourceFile("/exampleWithSalesforceTag.json").toByteArray()),
                    ),
                ),
            )
        }
        verify { kafkaConsumerMock.commitSync() }
    }

    @Test
    fun `Duplicate removed - when consumer pulls two records with same key and value on the same poll call that work session should result in one post call to salesforce without the duplicate`() {
        val pollResponse1 = listOf(exampleWithSalesforceTagRecord, exampleWithSalesforceTagWithOffset1Record).toConsumerRecords() // 2 records, one duplicate except offset
        val pollResponse2 = ConsumerRecords<String, String?>(mapOf())

        every { kafkaConsumerMock.poll(any<Duration>()) } returnsMany listOf(pollResponse1, pollResponse2)

        kafkaToSFPoster.runWorkSession()

        verify(exactly = 1) {
            sfClientMock.postRecords(
                setOf(
                    KafkaMessage(
                        CRM_Topic__c = "topic",
                        CRM_Key__c = "key",
                        CRM_Value__c = Base64.getEncoder().encodeToString(readResourceFile("/exampleWithSalesforceTag.json").toByteArray()),
                    ),
                ),
            )
        }
        verify { kafkaConsumerMock.commitSync() }
    }

    @Test
    fun `Seek flag - should make consumer seek on first work session`() {
        setUp(flagSeek = true)

        val pollResponse1 = ConsumerRecords<String, String?>(mapOf()) // Result of first poll call - no records

        every { kafkaConsumerMock.seek(any(), 0L) } returns Unit
        every { kafkaConsumerMock.poll(any<Duration>()) } returns pollResponse1

        kafkaToSFPoster.runWorkSession()

        verify(exactly = 1) { kafkaConsumerMock.seek(any(), 0L) }
    }

    @Test
    fun `Flag no post - should not post anything but still commit when there is a record from poll`() {
        setUp(flagNoPost = true)

        val pollResponse1 = listOf(exampleWithSalesforceTagRecord).toConsumerRecords() // Result of first poll call
        val pollResponse2 = ConsumerRecords<String, String?>(mapOf()) // Result of second poll call - no records

        every { kafkaConsumerMock.poll(any<Duration>()) } returnsMany listOf(pollResponse1, pollResponse2)

        kafkaToSFPoster.runWorkSession()

        verify(exactly = 0) { sfClientMock.postRecords(any()) }
        verify { kafkaConsumerMock.commitSync() }
    }

    @Test
    fun `Filter - Like Two polls test case with a filter that lets everything through should result in the same postRecord calls`() {
        filter = filterMock
        setUp()

        // Apply a filter that lets all records through
        every { filterMock(any()) } returns true

        // Setup mock responses to poll:
        val pollResponse1 = listOf(exampleWithSalesforceTagRecord).toConsumerRecords() // Result of first poll call - one record
        val pollResponse2 = listOf(exampleWithSalesforceTagWithOffset1Record).toConsumerRecords() // Result of second poll call - one record
        val pollResponse3 = ConsumerRecords<String, String?>(mapOf()) // Result of third poll call - no records

        every { kafkaConsumerMock.poll(any<Duration>()) } returnsMany listOf(pollResponse1, pollResponse2, pollResponse3)

        kafkaToSFPoster.runWorkSession()

        verify(exactly = 2) { filterMock(any()) }
        verify(exactly = 2) {
            sfClientMock.postRecords(
                setOf(
                    KafkaMessage(
                        CRM_Topic__c = "topic",
                        CRM_Key__c = "key",
                        CRM_Value__c = Base64.getEncoder().encodeToString(readResourceFile("/exampleWithSalesforceTag.json").toByteArray()),
                    ),
                ),
            )
        }
        verify { kafkaConsumerMock.commitSync() }
    }

    @Test
    fun `Filter - Like Two polls test case with a filter that lets nothing through should result in no postRecord calls`() {
        filter = filterMock
        setUp()

        // Apply a filter that lets nothing through
        every { filterMock(any()) } returns false

        // Setup mock responses to poll:
        val pollResponse1 = listOf(exampleWithSalesforceTagRecord).toConsumerRecords() // Result of first poll call - one record
        val pollResponse2 = listOf(exampleWithSalesforceTagWithOffset1Record).toConsumerRecords() // Result of second poll call - one record
        val pollResponse3 = ConsumerRecords<String, String?>(mapOf()) // Result of third poll call - no records

        every { kafkaConsumerMock.poll(any<Duration>()) } returnsMany listOf(pollResponse1, pollResponse2, pollResponse3)

        kafkaToSFPoster.runWorkSession()

        verify(exactly = 2) { filterMock(any()) }
        verify(exactly = 0) {
            sfClientMock.postRecords(
                setOf(
                    KafkaMessage(
                        CRM_Topic__c = "topic",
                        CRM_Key__c = "key",
                        CRM_Value__c = Base64.getEncoder().encodeToString(readResourceFile("/exampleWithSalesforceTag.json").toByteArray()),
                    ),
                ),
            )
        }
        verify(exactly = 2) { kafkaConsumerMock.commitSync() }
    }

    @Test
    fun `Modifier - Like Two polls test case with a modifier - should result in the same postRecord calls with the modification applied to record values`() {
        modifier = { it.value().toString() + "-modification" }
        Assertions.assertEquals("value-modification", modifier!!("value".asRecordValue()))
        setUp()

        // Apply a filter that lets all records through
        every { filterMock(any()) } returns true

        // Setup mock responses to poll:
        val pollResponse1 = listOf(exampleWithSalesforceTagRecord).toConsumerRecords() // Result of first poll call - one record
        val pollResponse2 = listOf(exampleWithSalesforceTagWithOffset1Record).toConsumerRecords() // Result of second poll call - one record
        val pollResponse3 = ConsumerRecords<String, String?>(mapOf()) // Result of third poll call - no records

        every { kafkaConsumerMock.poll(any<Duration>()) } returnsMany listOf(pollResponse1, pollResponse2, pollResponse3)

        kafkaToSFPoster.runWorkSession()

        verify(exactly = 2) {
            sfClientMock.postRecords(
                setOf(
                    KafkaMessage(
                        CRM_Topic__c = "topic",
                        CRM_Key__c = "key",
                        CRM_Value__c =
                            Base64.getEncoder().encodeToString(
                                (
                                    readResourceFile("/exampleWithSalesforceTag.json") +
                                        "-modification"
                                ).toByteArray(),
                            ),
                    ),
                ),
            )
        }
        verify { kafkaConsumerMock.commitSync() }
    }
}
