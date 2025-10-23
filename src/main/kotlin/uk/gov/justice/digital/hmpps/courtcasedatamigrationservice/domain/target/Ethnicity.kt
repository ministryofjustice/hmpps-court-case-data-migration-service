package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

data class Ethnicity(
  val id: Int,
  val observedEthnicityId: Int?,
  val observedEthnicityCode: String?,
  val observedEthnicityDescription: String?,
  val selfDefinedEthnicityId: Int?,
  val selfDefinedEthnicityCode: String?,
  val selfDefinedEthnicityDescription: String?,
  val createdAt: String?,
  val createdBy: String?,
  val updatedAt: String?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
