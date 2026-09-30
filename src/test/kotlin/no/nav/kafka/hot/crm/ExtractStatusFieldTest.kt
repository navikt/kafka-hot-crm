package no.nav.kafka.hot.crm

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class ExtractStatusFieldTest {
    private fun String?.asRecordValue() = ConsumerRecord("hot-crm.payment-status", 0, 0L, "key", this)

    @Test
    fun extracts_status_value_from_json_payload() {
        val record = """{"status":"OK"}""".asRecordValue()
        assertEquals("OK", extractStatusField(record))
    }

    @Test
    fun returns_null_for_tombstone() {
        val record = null.asRecordValue()
        assertEquals(null, extractStatusField(record))
    }

    @Test
    fun returns_null_when_status_is_json_null() {
        val record = """{"status":null}""".asRecordValue()
        assertEquals(null, extractStatusField(record))
    }
}
