package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class CourtCaseDataMigrationService

fun main(args: Array<String>) {
  runApplication<CourtCaseDataMigrationService>(*args)
}
