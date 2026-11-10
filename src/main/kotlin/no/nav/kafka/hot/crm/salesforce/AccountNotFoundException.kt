package no.nav.kafka.hot.crm.salesforce

class AccountNotFoundException : RuntimeException("Account query returned 404. Check that the personIdent field is correct")
