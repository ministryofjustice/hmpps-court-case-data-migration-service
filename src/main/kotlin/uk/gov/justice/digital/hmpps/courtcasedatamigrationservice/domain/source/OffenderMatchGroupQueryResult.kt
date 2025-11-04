package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source

import java.sql.Timestamp

data class OffenderMatchGroupQueryResult(
  val id: Int,
  val caseId: Int?,
  val defendantId: Int?,
  val created: Timestamp?,
  val createdBy: String?,
  val lastUpdated: Timestamp?,
  val lastUpdatedBy: String?,
  val deleted: Boolean?,
  val version: Int?,
)
