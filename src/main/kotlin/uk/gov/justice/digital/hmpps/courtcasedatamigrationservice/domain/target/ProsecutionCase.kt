package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp
import java.util.UUID

data class ProsecutionCase(
  val id: UUID,
  val legacyID: Long?,
  val caseID: String?,
  val caseURN: String?,
  val cID: Int?,
  val sourceType: String?,
  val caseMarkers: String?,
  val caseDocuments: String?,
  val createdAt: Timestamp?,
  val createdBy: String?,
  val updatedAt: Timestamp?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
