package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet

interface PostMigrationValidationStrategy {
  fun fetchSourceRecord(id: Long): Map<String, Any>?
  fun fetchTargetRecord(id: Long): Map<String, Any>?
  fun compareRecords(source: Map<String, Any>, target: Map<String, Any>): List<String>
}
