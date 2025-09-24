package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "batch")
data class BatchProperties(val chunkSize: Int)
