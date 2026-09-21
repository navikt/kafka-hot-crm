package no.nav.kafka.hot.crm.test

import com.google.gson.Gson
import no.nav.kafka.hot.crm.salesforce.SalesforceClient
import org.http4k.core.HttpHandler
import org.http4k.core.Response
import org.http4k.core.Status

object Test {
    private val gson = Gson()

    data class TryOutSFCallRequest(
        val action: String? = null,
        val topic: String? = null,
        val externalId: String? = null,
        val value: String? = null,
        val sObjectType: String? = null,
    )

    val tryOutSFRestHandler: HttpHandler = { request ->
        val body =
            runCatching {
                gson.fromJson(request.bodyString(), TryOutSFCallRequest::class.java)
            }.getOrNull()

        if (body == null) {
            Response(Status.BAD_REQUEST).body("Invalid JSON body")
        } else {
            val sfClient = SalesforceClient()
            val action = body.action?.lowercase() ?: "update"

            val response =
                when (action) {
                    "describe" ->
                        sfClient.describeSObject(body.sObjectType ?: "HOT_Claim__c")
                    "update" ->
                        if (body.topic.isNullOrBlank() || body.externalId.isNullOrBlank()) {
                            Response(Status.BAD_REQUEST)
                                .body("Missing required fields for update: topic, externalId")
                        } else {
                            sfClient.updateField(
                                topic = body.topic,
                                externalId = body.externalId,
                                value = body.value,
                            )
                        }
                    else ->
                        Response(Status.BAD_REQUEST)
                            .body("Unsupported action '$action'. Supported actions: update, describe")
                }

            Response(response.status).body(response.bodyString())
        }
    }
}
