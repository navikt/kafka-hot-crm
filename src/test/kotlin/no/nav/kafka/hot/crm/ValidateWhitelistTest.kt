package no.nav.kafka.hot.crm

import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.google.gson.JsonPrimitive
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class ValidateWhitelistTest {
    private val dev = JsonParser.parseString(readResourceFile("/whitelist/dev.json"))
    private val prod = JsonParser.parseString(readResourceFile("/whitelist/prod.json"))

    @Test
    fun whitelist_json_should_be_json_object() {
        assertEquals(true, dev is JsonObject)
        assertEquals(true, prod is JsonObject)
    }

    private fun containsOnlyJsonObjectOrAllValue(element: JsonElement): Boolean {
        if (element is JsonPrimitive) {
            return element.asString == "ALL"
        }
        if (element is JsonObject) {
            return element.entrySet().all { containsOnlyJsonObjectOrAllValue(it.value) }
        }
        return false
    }

    @Test
    fun whitelist_json_should_only_contain_json_objects_and_primitive_ALL() {
        assertEquals(true, containsOnlyJsonObjectOrAllValue(dev))
        assertEquals(true, containsOnlyJsonObjectOrAllValue(prod))
    }
}
