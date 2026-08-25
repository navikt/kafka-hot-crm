package no.nav.kafka.hot.crm.gui

import no.nav.kafka.hot.crm.readResourceFile
import no.nav.kafka.hot.crm.reduceByWhitelistAndRemoveHistoricalItems
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.http4k.core.Method
import org.http4k.core.Request
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class GuiTest {
    private val exampleWithSalesforceTagRecord = readResourceFile("/exampleWithSalesforceTag.json").asRecordValue()

    private fun String?.asRecordValue() = ConsumerRecord("topic", 0, 0L, "key", this)

    @Test
    fun testExpectedResult() {
        val whitelist = """
            {
              "hentPerson": {
                  "bostedsadresse": {
                    "vegadresse" : {
                        "husnummer": "ALL",
                        "husbokstav": "ALL"
                    }
                  },
                  "foedselsdato": {
                    "metadata": {
                        "master": "ALL"
                    }
                  }
              }
            }
        """

        // Will update Gui models:
        reduceByWhitelistAndRemoveHistoricalItems(exampleWithSalesforceTagRecord, whitelist)

        val response = Gui.guiHandler.invoke(Request(Method.GET, "", ""))

        val expectedResultPage = readResourceFile("/GuiPageFromTestCase.html").replace("\n", System.lineSeparator())

        Assertions.assertEquals(expectedResultPage, response.body.toString())
    }
}
