package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.config.BatchProperties

@SpringBootApplication
@EnableConfigurationProperties(BatchProperties::class)
class CourtCaseDataMigrationService

fun main(args: Array<String>) {
  runApplication<CourtCaseDataMigrationService>(*args)
}
