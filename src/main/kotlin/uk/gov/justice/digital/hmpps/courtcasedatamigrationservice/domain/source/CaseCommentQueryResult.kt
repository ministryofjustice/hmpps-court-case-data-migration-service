package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source

import java.sql.Timestamp
import java.util.UUID

data class CaseCommentQueryResult(
  val id: Int,
  val caseID: String?,
  val defendantID: UUID?,
  val author: String?,
  val comment: String?,
  val isDraft: Boolean?,
  val legacy: Boolean?,
  val created: Timestamp?,
  val createdBy: String?,
  val lastUpdated: Timestamp?,
  val lastUpdatedBy: String?,
  val deleted: Boolean?,
  val version: Int?,
)
