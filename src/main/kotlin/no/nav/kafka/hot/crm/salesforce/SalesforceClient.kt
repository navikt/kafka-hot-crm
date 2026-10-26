package no.nav.kafka.hot.crm.salesforce

import com.google.gson.Gson
import com.google.gson.JsonParser
import org.http4k.client.OkHttp
import org.http4k.core.Headers
import org.http4k.core.HttpHandler
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Response
import org.http4k.core.Status
import java.io.File

const val SALESFORCE_VERSION = "v61.0"

val externalIdField: Map<String, String> =
    mapOf(
        "payment-status" to "External_Id__c",
        "resource-number" to "INT_PersonIdent__c",
    )

private val gson = Gson()

/**
 * Maps a kafka topic to the HOT_Claim__c field that the topic value should be written to.
 */
val topicToClaimField: Map<String, String> =
    mapOf(
        "payment-status" to "PaymentStatus__c",
        "resource-number" to "HOT_ResourceNumber__c",
    )

class SalesforceClient(
    private val httpClient: HttpHandler = OkHttp(),
    private val accessTokenHandler: AccessTokenHandler = NewAccessTokenHandler(),
) {
    /*
    fun postRecords(kafkaMessages: Set<KafkaMessage>): Response {
        val requestBody = SFsObjectRest(records = kafkaMessages).toJson()

        val dstUrl = "${accessTokenHandler.instanceUrl}/services/data/$SALESFORCE_VERSION/composite/sobjects"

        val request = Request(Method.POST, dstUrl).headers(defaultHeaders()).body(requestBody)

        File("/tmp/files/latestPostRequest").writeText(request.toMessage())

        return httpClient(request)
    }
     */

    /**
     * Updates a single field on a HOT_Claim__c record, identified by its External_Id__c,
     * based on which kafka topic the message came from.
     */
    fun updateField(
        topic: String,
        externalId: String,
        value: String?,
    ): Response {
        val field =
            topicToClaimField[topic.substringAfterLast('.')]
                ?: throw IllegalArgumentException("No HOT_Claim__c field mapping for topic $topic")
        return when (topic.substringAfterLast('.')) {
            "payment-status" -> updatePaymentStatus(externalId, field, value)
            "resource-number" -> updateResourceNumber(field, value)
            else -> throw IllegalArgumentException("No HOT_Claim__c field mapping for topic $topic")
        }
    }

    private fun updatePaymentStatus(
        externalId: String,
        fieldName: String,
        value: String?,
    ): Response {
        val dstUrl =
            "${accessTokenHandler.instanceUrl}/services/data/$SALESFORCE_VERSION/sobjects/HOT_Claim__c/" +
                "${externalIdField["payment-status"]}/$externalId?updateOnly=true"

        val requestBody = gson.toJson(mapOf(fieldName to value))
        val request = Request(Method.PATCH, dstUrl).headers(defaultHeaders()).body(requestBody)

        return httpClient(request)
    }

    private fun updateResourceNumber(
        fieldName: String,
        payload: String?,
    ): Response {
        val json = JsonParser.parseString(payload).asJsonObject
        val personIdent = json["personIdent"].asString
        val resourceNumber = json["resourceNumber"].asString

        log.info("Updating resource number for $personIdent with $resourceNumber")

        val personId = findPersonRecordId(personIdent)
        if (personId == null) throw IllegalArgumentException("Person not found for resource number update")

        val dstUrl = "${accessTokenHandler.instanceUrl}/services/data/$SALESFORCE_VERSION/sobjects/Person__c/$personId"
        val body = gson.toJson(mapOf(fieldName to resourceNumber))

        val request = Request(Method.PATCH, dstUrl).headers(defaultHeaders()).body(body)
        return httpClient(request)
    }

    private fun findPersonRecordId(externalId: String): String? {
        val accountDetails: Response = getAccountDetails(externalId)

        if (!accountDetails.status.successful) {
            val statusCode = accountDetails.status.code
            val reason = Status.fromCode(statusCode)?.description ?: "Unknown Status"
            throw IllegalStateException(
                "Failed to get account details: HTTP $statusCode $reason; ${salesforceErrorCodes(accountDetails)}",
            )
        }

        return JsonParser
            .parseString(accountDetails.bodyString())
            .asJsonObject
            .get("CRM_Person__c")
            ?.takeUnless { it.isJsonNull }
            ?.asString
    }

    private fun salesforceErrorCodes(response: Response): String {
        // Only allow bounded codes into public exception logs, never response text or parser exceptions.
        val codes =
            runCatching {
                val body = response.bodyString()
                require(body.length <= 16_384)
                JsonParser
                    .parseString(body)
                    .asJsonArray
                    .map { error ->
                        val code = error.asJsonObject.getAsJsonPrimitive("errorCode")
                        require(code != null && code.isString)
                        code.asString.also { require(it.matches(Regex("[A-Z_][A-Z_0-9]{0,63}"))) }
                    }.distinct()
                    .take(5)
            }.getOrNull()

        return if (codes.isNullOrEmpty()) {
            "no readable Salesforce error code"
        } else {
            "Salesforce errorCode(s): ${codes.joinToString(", ")}"
        }
    }

    private fun getAccountDetails(externalId: String): Response {
        val dstUrl =
            "${accessTokenHandler.instanceUrl}/services/data/$SALESFORCE_VERSION/sobjects/Account/${externalIdField["resource-number"]}/$externalId"
        val request = Request(Method.GET, dstUrl).headers(defaultHeaders())

        return httpClient(request)
    }

    /**
     * Returns Salesforce object metadata from the describe endpoint.
     */
    fun describeSObject(sObjectType: String = "HOT_Claim__c"): Response {
        val objectName = sObjectType.trim()
        require(objectName.isNotEmpty()) { "sObjectType must not be blank" }

        val dstUrl =
            "${accessTokenHandler.instanceUrl}/services/data/$SALESFORCE_VERSION/sobjects/$objectName/describe"

        val request = Request(Method.GET, dstUrl).headers(defaultHeaders())
        return httpClient(request)
    }

    /**
     * Returns metadata for all sObjects available to the authenticated user.
     */
    fun describeGlobal(): Response {
        val dstUrl = "${accessTokenHandler.instanceUrl}/services/data/$SALESFORCE_VERSION/sobjects"
        val request = Request(Method.GET, dstUrl).headers(defaultHeaders())
        return httpClient(request)
    }

    /**
     * Returns the runtime Salesforce auth context used by this app.
     */
    fun context(): Response {
        val payload =
            gson.toJson(
                mapOf(
                    "instanceUrl" to accessTokenHandler.instanceUrl,
                    "tenantId" to accessTokenHandler.tenantId,
                    "apiVersion" to SALESFORCE_VERSION,
                ),
            )
        return Response(Status.OK).body(payload)
    }

    private fun defaultHeaders(): Headers =
        listOf(
            "Authorization" to "Bearer ${accessTokenHandler.accessToken}",
            "Content-Type" to "application/json;charset=UTF-8",
        )
}
