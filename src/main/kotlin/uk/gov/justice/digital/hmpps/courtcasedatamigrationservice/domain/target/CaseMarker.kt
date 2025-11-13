package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

data class CaseMarker(
  val id: Int,
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
