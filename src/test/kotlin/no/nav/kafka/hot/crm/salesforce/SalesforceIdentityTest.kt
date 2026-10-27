package no.nav.kafka.hot.crm.salesforce

import com.google.gson.JsonParser
import org.http4k.core.HttpHandler
import org.http4k.core.Method
import org.http4k.core.Response
import org.http4k.core.Status
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class SalesforceIdentityTest {
    private val tokenHandler =
        object : AccessTokenHandler {
            override val accessToken = "fake-token"
            override val instanceUrl = "https://salesforce.invalid"
            override val tenantId = "fake-tenant"
        }

    @Test
    fun `identity uses authenticated GET and returns only bounded IDs`() {
        var calls = 0
        val httpClient: HttpHandler = { request ->
            calls++
            assertEquals(Method.GET, request.method)
            assertEquals("https://salesforce.invalid/services/oauth2/userinfo", request.uri.toString())
            assertEquals("Bearer fake-token", request.header("Authorization"))
            Response(Status.OK).body(
                """{
                    "user_id":"005000000000001AAA",
                    "organization_id":"00D000000000001AAA",
                    "preferred_username":"sensitive-username",
                    "email":"sensitive-email",
                    "name":"sensitive-name",
                    "access_token":"sensitive-token",
                    "urls":{"custom_domain":"https://sensitive.invalid"}
                }""",
            )
        }

        val response = SalesforceClient(httpClient, tokenHandler).identity()

        assertEquals(1, calls)
        assertEquals(Status.OK, response.status)
        assertEquals(
            JsonParser.parseString("""{"userId":"005000000000001AAA","organizationId":"00D000000000001AAA"}"""),
            JsonParser.parseString(response.bodyString()),
        )
    }

    @Test
    fun `identity accepts 15 character Salesforce IDs`() {
        val client =
            SalesforceClient(
                { Response(Status.OK).body("""{"user_id":"005000000000001","organization_id":"00D000000000001"}""") },
                tokenHandler,
            )

        assertEquals(
            """{"userId":"005000000000001","organizationId":"00D000000000001"}""",
            client.identity().bodyString(),
        )
    }

    @Test
    fun `failed identity exposes only status and safe Salesforce codes`() {
        val cases =
            listOf(
                Triple(
                    Status.UNAUTHORIZED,
                    """[{"errorCode":"INVALID_SESSION_ID","message":"sensitive-token"}]""",
                    "HTTP 401 Unauthorized; Salesforce errorCode(s): INVALID_SESSION_ID",
                ),
                Triple(
                    Status(502, "sensitive-custom-reason"),
                    "<html>sensitive-body</html>",
                    "HTTP 502 Bad Gateway; no readable Salesforce error code",
                ),
                Triple(
                    Status.BAD_REQUEST,
                    """[{"errorCode":"sensitive arbitrary error","message":"sensitive-body"}]""",
                    "HTTP 400 Bad Request; no readable Salesforce error code",
                ),
            )
        cases.forEach { (status, body, expected) ->
            val client = SalesforceClient({ Response(status).body(body) }, tokenHandler)

            val failure = assertThrows(IllegalStateException::class.java) { client.identity() }

            assertEquals("Failed to get Salesforce identity: $expected", failure.message)
            assertNull(failure.cause)
        }
    }

    @Test
    fun `malformed or missing identity fails without response or parser details`() {
        listOf(
            "sensitive malformed body",
            "null",
            "[]",
            "{}",
            """{"user_id":"005000000000001AAA"}""",
            """{"organization_id":"00D000000000001AAA"}""",
            """{"user_id":null,"organization_id":"00D000000000001AAA"}""",
            """{"user_id":123,"organization_id":"00D000000000001AAA"}""",
            """{"user_id":{},"organization_id":"00D000000000001AAA"}""",
            """{"user_id":"sensitive-user","organization_id":"00D000000000001AAA"}""",
            """{"user_id":"005000000000001AAA","organization_id":""}""",
            """{"user_id":"005000000000001AAA","organization_id":"https://sensitive.invalid"}""",
            """{"user_id":"${"x".repeat(16_384)}","organization_id":"00D000000000001AAA"}""",
        ).forEach { body ->
            val client = SalesforceClient({ Response(Status.OK).body(body) }, tokenHandler)

            val failure = assertThrows(IllegalStateException::class.java) { client.identity() }

            assertEquals("Invalid Salesforce identity response", failure.message)
            assertNull(failure.cause)
        }
    }
}
