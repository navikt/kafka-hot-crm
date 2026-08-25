package no.nav.kafka.hot.crm.gui

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.JsonObject
import org.http4k.core.HttpHandler
import org.http4k.core.Response
import org.http4k.core.Status
import java.lang.IllegalStateException

object Gui {
    val prettifier: Gson = GsonBuilder().setPrettyPrinting().create()
    var latestMessageAndRemovalSets: Pair<Set<List<String>>, Set<List<String>>> = Pair(setOf(), setOf())

    val guiHandler: HttpHandler = {
        val latestCompleteMessageModel = parseFieldsListToJsonObject(latestMessageAndRemovalSets.first)
        val latestRemovalModel = parseFieldsListToJsonObject(latestMessageAndRemovalSets.second)
        val latestMarkedMessageModel = markRemovedFields(latestCompleteMessageModel, latestRemovalModel)
        Response(Status.OK).body(generateHTMLFromPrettifiedMarkedJSON(prettifier.toJson(latestMarkedMessageModel)))
    }

    /**
     * parseFieldsListToJsonObject
     * Translates set of field names to a JsonObject (where nodes are only JsonObjects)
     */
    fun parseFieldsListToJsonObject(fields: Set<List<String>>): JsonObject {
        val jsonObject = JsonObject()

        for (fieldHierarchy in fields) {
            var currentObject = jsonObject

            for (fieldName in fieldHierarchy) {
                if (!currentObject.has(fieldName)) {
                    val newObject = JsonObject()
                    currentObject.add(fieldName, newObject)
                    currentObject = newObject
                } else {
                    currentObject = currentObject.getAsJsonObject(fieldName)
                }
            }
        }

        return jsonObject
    }

    /**
     * markRemovedFields - a recursive function that takes a message model and a remove model as json objects
     * and produces a marked message model where fields to be removed are marked with a '!' prefix, i.e "!tags"
     */
    private fun markRemovedFields(
        message: JsonObject,
        removeObject: JsonObject,
        resultHolder: JsonObject = JsonObject(),
        mark: Boolean = false,
    ): JsonObject {
        for ((key, value) in message.entrySet()) {
            if (value is JsonObject) {
                val obj = JsonObject()
                val exitsInRemove = removeObject.keySet().contains(key)
                val shouldMark = mark || (exitsInRemove && removeObject[key].asJsonObject.size() == 0)

                val markString = if (shouldMark) "!" else ""
                resultHolder.add(markString + key, obj)
                markRemovedFields(value.asJsonObject, if (exitsInRemove) removeObject[key].asJsonObject else JsonObject(), obj, shouldMark)
            } else {
                throw IllegalStateException("Message model should only include json objects - but does not!")
            }
        }
        return resultHolder
    }

    /**
     * generateHTMLFromPrettifiedMarkedJSON - takes a prettified json string where field names to be removed starts with '!'
     * and produces html for display
     */
    private fun generateHTMLFromPrettifiedMarkedJSON(jsonString: String): String {
        val jsonLines = jsonString.split("\n")
        return wrapWithHTMLPage(buildHTMLContentFromJsonLines(jsonLines))
    }

    private fun wrapWithHTMLPage(content: String): String =
        """
        <html>
        <head>
        <style>
            .toberemoved { background-color: #ffcccc; }
        </style>
        </head>
        <body>
        $content
        </body>
        </html>
        """.trimIndent()

    private fun buildHTMLContentFromJsonLines(jsonLines: List<String>): String {
        val htmlContent = StringBuilder()
        htmlContent.append("<pre>")
        for (line in jsonLines) {
            if (line.isNotBlank()) {
                val trimmedLine = line.trim()
                val spanClass = if (trimmedLine.startsWith("\"!")) " class=\"toberemoved\"" else ""
                val sanitizedLine = line.replace("!", "")
                htmlContent.append("<span$spanClass>$sanitizedLine</span><br>")
            }
        }
        htmlContent.append("</pre>")
        return htmlContent.toString()
    }
}
