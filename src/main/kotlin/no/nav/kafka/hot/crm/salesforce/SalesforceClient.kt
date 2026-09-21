package no.nav.kafka.hot.crm.salesforce

import com.google.gson.Gson
import mu.KotlinLogging
import no.nav.kafka.hot.crm.config_DEPLOY_APP
import no.nav.kafka.hot.crm.devContext
import no.nav.kafka.hot.crm.env
import org.http4k.client.OkHttp
import org.http4k.core.Headers
import org.http4k.core.HttpHandler
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Response
import java.io.File

const val SALESFORCE_VERSION = "v57.0"

const val HOT_CLAIM_EXTERNAL_ID_FIELD = "External_Id__c"

private val log = KotlinLogging.logger { }

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
    fun postRecords(kafkaMessages: Set<KafkaMessage>): Response {
        val requestBody = SFsObjectRest(records = kafkaMessages).toJson()

        val dstUrl = "${accessTokenHandler.instanceUrl}/services/data/$SALESFORCE_VERSION/composite/sobjects"

        val headers: Headers =
            listOf(
                "Authorization" to "Bearer ${accessTokenHandler.accessToken}",
                "Content-Type" to "application/json;charset=UTF-8",
            )

        val request = Request(Method.POST, dstUrl).headers(headers).body(requestBody)

        File("/tmp/files/latestPostRequest").writeText(request.toMessage())

        return httpClient(request)
    }

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
                "$HOT_CLAIM_EXTERNAL_ID_FIELD/$externalId"

        val headers: Headers =
            listOf(
                "Authorization" to "Bearer ${accessTokenHandler.accessToken}",
                "Content-Type" to "application/json;charset=UTF-8",
            )

        val requestBody = gson.toJson(mapOf(fieldName to value))

        val request = Request(Method.PATCH, dstUrl).headers(headers).body(requestBody)

        return httpClient(request)
    }
}
