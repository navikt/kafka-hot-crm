package no.nav.kafka.hot.crm.salesforce

interface AccessTokenHandler {
    val accessToken: String
    val instanceUrl: String
    val tenantId: String
}
