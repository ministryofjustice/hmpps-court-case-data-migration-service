package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp

data class CourtCentre(
  val id: Int,
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
