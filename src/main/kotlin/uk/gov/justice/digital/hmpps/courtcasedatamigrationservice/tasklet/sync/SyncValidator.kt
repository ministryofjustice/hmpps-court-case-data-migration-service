package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.sync

abstract class SyncValidator {
  abstract fun fetchSourceIDs(minId: Long, maxId: Long, sampleSize: Int): List<Long>
  abstract fun fetchSourceRecord(id: Long): Map<String, Any>?
  abstract fun fetchTargetRecord(map: Map<String, Any>): Map<String, Any>?
}
