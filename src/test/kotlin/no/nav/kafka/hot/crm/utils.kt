@file:Suppress("ktlint:standard:filename")

package no.nav.kafka.hot.crm

fun readResourceFile(path: String) = KafkaPosterApplication::class.java.getResource(path).readText()
