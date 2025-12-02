package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.util.UUID

data class CaseDocument(
  val id: UUID,
  val legacyID: Long?,
  val documentID: String?,
  val documentName: String?,
  val createdAt: String?,
  val createdBy: String?,
  val updatedAt: String?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
