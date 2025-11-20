package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service

import org.springframework.stereotype.Service
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.util.SqlFileExecutor

@Service
class DatabaseMaintenanceService(private val sqlFileExecutor: SqlFileExecutor) {

  fun truncateTables() {
    sqlFileExecutor.executeVoid("sql/truncate_tables.sql")
  }

  fun countMigratedRecords(): Long = sqlFileExecutor.executeScalar("sql/count_migrated_records.sql", Long::class.java) ?: 0

  fun dropForeignKeys() {
    sqlFileExecutor.executeVoid("sql/drop_foreign_keys.sql")
  }

  fun recreateForeignKeys() {
    sqlFileExecutor.executeVoid("sql/recreate_foreign_keys.sql")
  }

  fun findForeignKeys(): String {
    val executeQuery = sqlFileExecutor.executeQuery("sql/find_foreign_keys.sql")
    val foundForeignKeysText = executeQuery.joinToString(separator = "\n") { row ->
      "Foreign Key $row."
    }
    return foundForeignKeysText.ifEmpty { "No Foreign keys found." }
  }
}
