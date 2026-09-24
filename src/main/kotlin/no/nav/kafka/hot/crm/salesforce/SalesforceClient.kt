package no.nav.kafka.hot.crm.salesforce

import com.google.gson.Gson
import org.http4k.client.OkHttp
import org.http4k.core.Headers
import org.http4k.core.HttpHandler
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Response
import org.http4k.core.Status
import java.io.File

const val SALESFORCE_VERSION = "v61.0"

const val HOT_CLAIM_EXTERNAL_ID_FIELD = "External_Id__c"

private val gson = Gson()

/**
 * Maps a kafka topic to the HOT_Claim__c field that the topic value should be written to.
 */
val topicToClaimField: Map<String, String> =
    mapOf(
        "payment-status" to "PaymentStatus__c",
        "resource-number" to "ResourceNumber__c",
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
        val fieldName =
            topicToClaimField[topic.substringAfterLast('.')]
                ?: throw IllegalArgumentException("No HOT_Claim__c field mapping for topic $topic")

        val dstUrl =
            "${accessTokenHandler.instanceUrl}/services/data/$SALESFORCE_VERSION/sobjects/HOT_Claim__c/" +
                "$HOT_CLAIM_EXTERNAL_ID_FIELD/$externalId?updateOnly=true"

        val requestBody = gson.toJson(mapOf(fieldName to value))
        val request = Request(Method.PATCH, dstUrl).headers(defaultHeaders()).body(requestBody)

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
