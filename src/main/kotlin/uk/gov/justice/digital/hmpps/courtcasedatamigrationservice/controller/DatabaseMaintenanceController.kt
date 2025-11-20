package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.controller

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service.DatabaseMaintenanceService

@RestController
@RequestMapping("/db-maintenance")
class DatabaseMaintenanceController(private val databaseMaintenanceService: DatabaseMaintenanceService) {

  private companion object {
    val log: Logger = LoggerFactory.getLogger(DatabaseMaintenanceController::class.java)
  }

  @PostMapping("/truncate-tables")
  fun truncateTables(): ResponseEntity<String> {
    databaseMaintenanceService.truncateTables()
    val response = "Truncated tables. Total migrated records count -> ${databaseMaintenanceService.countMigratedRecords()}."
    log.info("Response: $response")
    return ResponseEntity.ok(response)
  }

  @GetMapping("/count-migrated-records")
  fun countMigratedRecords(): ResponseEntity<String> {
    val response = "Total migrated records count -> ${databaseMaintenanceService.countMigratedRecords()}"
    log.info("Response: $response")
    return ResponseEntity.ok(response)
  }

  @GetMapping("/list-foreign-keys")
  fun listForeignKeys(): ResponseEntity<String> {
    val response = "Foreign keys found in the database.\n${databaseMaintenanceService.findForeignKeys()}"
    log.info("Response: $response")
    return ResponseEntity.ok(response)
  }

  @PostMapping("/drop-foreign-keys")
  fun dropForeignKeys(): ResponseEntity<String> {
    databaseMaintenanceService.dropForeignKeys()
    val response = "Foreign keys dropped successfully.\n${databaseMaintenanceService.findForeignKeys()}"
    log.info("Response: $response")
    return ResponseEntity.ok(response)
  }

  @PostMapping("/recreate-foreign-keys")
  fun recreateForeignKeys(): ResponseEntity<String> {
    databaseMaintenanceService.recreateForeignKeys()
    val response = "Foreign keys recreated successfully.\n${databaseMaintenanceService.findForeignKeys()}"
    log.info("Response: $response")
    return ResponseEntity.ok(response)
  }
}
