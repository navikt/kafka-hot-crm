@file:Suppress("ktlint:standard:filename")

package no.nav.kafka.hot.crm

import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonNull
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.google.gson.JsonPrimitive
import no.nav.kafka.hot.crm.gui.Gui
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

// Only read once, first time it is used
val whitelistText: String by lazy {
    KafkaPosterApplication::class.java.getResource(env(config_WHITELIST_FILE)).readText()
}

fun reduceByWhitelistAndRemoveHistoricalItems(
    record: ConsumerRecord<String, String?>,
    whitelist: String = whitelistText,
): String? {
    if (record.value() == null) {
        File("/tmp/tombstones").appendText("${record.key()} at $currentTimeTag\n")
        return null // Tombstone - no reduction to be made
    }
    try {
        val whitelistObject = JsonParser.parseString(whitelist) as JsonObject
        val messageObject = JsonParser.parseString(record.value()) as JsonObject

        val removeList = findNonWhitelistedFields(whitelistObject, messageObject)

        File("/tmp/latestDroppedPdlFields").writeText(
            removeList.map { it.joinToString(".") }.joinToString("\n"),
        )

        val allList = listAllFields(messageObject)

        Gui.latestMessageAndRemovalSets = Pair(allList, removeList)

        removeList.forEach {
            messageObject.removeFields(it)
        }

        return removeHistoricalItems(messageObject.toString())
    } catch (e: Exception) {
        File("/tmp/reducebywhitelistfail").appendText("$record\n\n")
        throw RuntimeException("Unable to parse event and filter to reduce by whitelist")
    }
}

val currentTimeTag: String get() = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME)

/**
 * findNonWhitelistedField
 * A function designed to identify and collect paths of non-whitelisted fields within a
 * JSON structure, based on a specified whitelist
 *
 * - whitelistElement: A JsonElement representing the whitelist JSON structure.
 *                     It serves as the reference for determining which fields are allowed.
 * - messageElement: A JsonElement representing the JSON structure to be analyzed.
 */
private fun findNonWhitelistedFields(
    whitelistElement: JsonElement,
    messageElement: JsonElement,
    resultHolder: MutableSet<List<String>> = mutableSetOf(),
    parents: List<String> = listOf(),
): Set<List<String>> {
    val whitelistEntrySet = (whitelistElement as JsonObject).entrySet()

    val messageEntrySet =
        if (messageElement is JsonArray) {
            messageElement.map { it as JsonObject }.flatMap { it.entrySet() }.toSet()
        } else {
            (messageElement as JsonObject).entrySet()
        }

    // Whitelist field with primitive value (typically "ALL") means allow field plus any subfields
    val whitelistPrimitives = whitelistEntrySet.filter { it.value is JsonPrimitive }.map { it.key }.toList()

    // Whitelist fields that contains another json object, means allow top field and the subobject will
    // describe what parts to allow for any subfields
    val whitelistObjects = whitelistEntrySet.filter { it.value is JsonObject }.map { it.key }.toList()

    val removeList =
        messageEntrySet
            .filter { entry ->
                // Never remove if fields is whitelisted as "ALL"
                if (whitelistPrimitives.contains(entry.key)) return@filter false

                // If not whitelisted as "ALL", remove any primitives and null
                if (entry.value is JsonPrimitive || entry.value is JsonNull) return@filter true

                // If field is object or array, only keep it if member of object whitelist
                !whitelistObjects.contains(entry.key)
            }.map { parents + it.key }

    resultHolder.addAll(removeList)

    // Apply recursively on any whitelist subnodes, given that the message node has corresponding array or object subnode
    whitelistEntrySet
        .filter { it.value is JsonObject }
        .forEach { whitelistEntry ->
            messageEntrySet
                .firstOrNull { it.key == whitelistEntry.key && (it.value is JsonObject || it.value is JsonArray) }
                ?.let { messageEntry ->
                    findNonWhitelistedFields(
                        whitelistEntry.value,
                        messageEntry.value,
                        resultHolder,
                        parents.toList() + whitelistEntry.key,
                    )
                }
        }
    return resultHolder
}

/**
 * JsonElement.removeField
 * Recursive extension function that facilitates the removal of a specified field within a JSON structure
 * It supports recursive removal, allowing the removal of nested fields.
 *
 * - fieldTree: A list of strings representing the path to the field to be removed.
 */
private fun JsonElement.removeFields(fieldTree: List<String>) {
    if (fieldTree.size == 1) {
        when (this) {
            is JsonObject -> this.remove(fieldTree.first())
            is JsonArray ->
                this.forEach {
                    (it as JsonObject).remove(fieldTree.first())
                }
            else -> {
                throw IllegalStateException("JsonElement.removeFieldRecurse attempted removing a primitive or null")
            }
        }
    } else {
        when (this) {
            is JsonObject -> this.get(fieldTree.first()).removeFields(fieldTree.subList(1, fieldTree.size))
            is JsonArray ->
                this.forEach {
                    (it as JsonObject).get(fieldTree.first()).removeFields(fieldTree.subList(1, fieldTree.size))
                }
            else -> {
                throw IllegalStateException("JsonElement.removeFieldRecurse attempted stepping into a primitive or null")
            }
        }
    }
}

/**
 * listAllFields
 * Recursive function that traverses a json object a returns a set of field paths (flat description of all fields).
 */
private fun listAllFields(
    messageElement: JsonElement,
    resultHolder: MutableSet<List<String>> = mutableSetOf(),
    parents: List<String> = listOf(),
): Set<List<String>> {
    val messageEntrySet =
        if (messageElement is JsonArray) {
            messageElement.filterIsInstance<JsonObject>().flatMap { it.entrySet() }.toSet()
        } else {
            (messageElement as JsonObject).entrySet()
        }

    val list = messageEntrySet.map { parents + it.key }

    resultHolder.addAll(list)

    messageEntrySet
        .filter { it.value is JsonObject || it.value is JsonArray }
        .forEach {
            listAllFields(
                it.value,
                resultHolder,
                parents.toList() + it.key,
            )
        }
    return resultHolder
}

fun removeHistoricalItems(jsonString: String): String {
    val jsonElement: JsonElement = JsonParser.parseString(jsonString)
    val jsonObject: JsonObject = jsonElement.asJsonObject

    if (!jsonObject.has("hentPerson")) {
        return jsonString
    }

    val hentPerson = jsonObject.getAsJsonObject("hentPerson")

    // Iterate over all fields in "hentPerson"
    for (entry in hentPerson.entrySet()) {
        val key = entry.key
        val value = entry.value

        // Check if the value is an array
        if (value.isJsonArray) {
            val jsonArray = value.asJsonArray
            val filteredArray = filterHistoricalEntries(jsonArray)
            hentPerson.add(key, filteredArray)
        }
    }

    // Return the updated JSON as a string
    return jsonObject.toString()
}

// Function to filter historical entries from an array
fun filterHistoricalEntries(array: JsonArray): JsonArray {
    val updatedArray = JsonArray()

    for (element in array) {
        val itemObject = element.asJsonObject
        if (itemObject.has("metadata")) {
            val metadata = itemObject.getAsJsonObject("metadata")
            // Check if "historisk" exists and is true
            val isHistorical = metadata.get("historisk")?.asBoolean ?: false
            if (!isHistorical) {
                updatedArray.add(itemObject) // Keep this entry if not historical
            }
        } else {
            updatedArray.add(itemObject) // Keep entries without metadata
        }
    }

    return updatedArray
}
