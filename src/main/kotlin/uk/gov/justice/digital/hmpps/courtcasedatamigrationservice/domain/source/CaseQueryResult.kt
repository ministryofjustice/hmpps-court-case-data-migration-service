package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source

import java.sql.Timestamp

data class CaseQueryResult(
  val id: Int,
  val urn: String?,
  val sourceType: String?,
  val created: Timestamp?,
  val createdBy: String?,
  val lastUpdated: Timestamp?,
  val lastUpdatedBy: String?,
  val deleted: Boolean?,
  val version: Int?,
  val caseDocuments: String?,
  val caseMarkers: String?,
)
