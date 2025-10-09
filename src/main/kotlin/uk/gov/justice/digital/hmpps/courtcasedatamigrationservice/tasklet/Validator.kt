package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet

interface Validator {
  fun fetchSourceIDs(minId: Long, maxId: Long, sampleSize: Int): List<Long>
  fun fetchSourceRecord(id: Long): Map<String, Any>?
  fun fetchTargetRecord(id: Long): Map<String, Any>?
  fun compareRecords(source: Map<String, Any>, target: Map<String, Any>): List<String>
}
