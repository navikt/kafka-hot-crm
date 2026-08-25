@file:Suppress("ktlint:standard:filename")

package no.nav.kafka.hot.crm

import com.google.gson.JsonArray
import com.google.gson.JsonNull
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.io.File

fun isTombstoneOrSalesforceTagged(record: ConsumerRecord<String, String?>): Boolean {
    try {
        if (record.value() == null) return true // Allow tombstone signal
        val obj = JsonParser.parseString(record.value()) as JsonObject
        if (obj["tags"] == null || obj["tags"] is JsonNull) return false
        return (obj["tags"] as JsonArray).any { it.asString == "SALESFORCE" }
    } catch (e: Exception) {
        File("/tmp/filterSalesforceTagFail").appendText("$record\nMESSAGE ${e.message}\n\n")
        throw RuntimeException("Unable to parse value for salesforce tag filter ${e.message}")
    }
}

fun hasVergemaalEllerFremtidsfullmakt(record: ConsumerRecord<String, String?>): Boolean {
    try {
        if (record.value() == null) return false // Allow tombstone signal
        val obj = JsonParser.parseString(record.value()) as JsonObject
        if (obj["hentPerson"] == null) return false
        val hentPerson = obj["hentPerson"] as JsonObject
        if (hentPerson["vergemaalEllerFremtidsfullmakt"] == null || hentPerson["vergemaalEllerFremtidsfullmakt"] is JsonNull) return false
        if (hentPerson["vergemaalEllerFremtidsfullmakt"] is JsonArray &&
            (hentPerson["vergemaalEllerFremtidsfullmakt"] as JsonArray).isEmpty
        ) {
            return false
        }
        return true
    } catch (e: Exception) {
        File("/tmp/filterHasVergemaalEllerFremtidsfullmaktFail").appendText("$record\nMESSAGE ${e.message}\n\n")
        throw RuntimeException("Unable to parse value for hasVergemaalEllerFremtidsfullmakt filter ${e.message}")
    }
}

val cherryPickList: List<Long> by lazy {
    KafkaPosterApplication::class.java
        .getResource("/offsetfile.txt")
        .readText()
        .split("\n")
        .map { it.trim().toLong() }
}

fun cherryPickOffsets(record: ConsumerRecord<String, String?>): Boolean {
    try {
        return cherryPickList.contains(record.offset())
    } catch (e: Exception) {
        File("/tmp/filterCherryPickFail").writeText("$record\nMESSAGE ${e.message}\n\n")
        throw RuntimeException("Unable to parse value for cherry pick filter ${e.message}")
    }
}
