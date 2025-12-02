package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.util.UUID

data class CaseMarker(
  val id: UUID,
  val legacyID: Long?,
  val typeID: String?,
  val typeCode: String?,
  val typeDescription: String?,
  val createdAt: String?,
  val createdBy: String?,
  val updatedAt: String?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
