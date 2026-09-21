package no.nav.kafka.hot.crm.test

import com.google.gson.Gson
import no.nav.kafka.hot.crm.salesforce.SalesforceClient
import org.http4k.core.HttpHandler
import org.http4k.core.Response

object Test {
    private val gson = Gson()

    data class TryOutSFCallRequest(
        val topic: String,
        val externalId: String,
        val value: String?,
    )

    val tryOutSFRestHandler: HttpHandler = { request ->
        val body = gson.fromJson(request.bodyString(), TryOutSFCallRequest::class.java)

        val response =
            SalesforceClient().updateField(
                topic = body.topic,
                externalId = body.externalId,
                value = body.value,
            )

        Response(response.status).body(response.bodyString())
    }
}