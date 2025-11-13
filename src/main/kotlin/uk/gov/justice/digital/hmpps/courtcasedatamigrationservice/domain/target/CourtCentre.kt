package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp
import java.util.UUID

data class CourtCentre(
  val id: UUID,
  val legacyID: Long?,
  val code: String?,
  val name: String?,
  val courtRooms: String?,
  val psaCode: String?,
  val region: String?,
  val address: String?,
  val createdAt: Timestamp?,
  val createdBy: String?,
  val updatedAt: Timestamp?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
