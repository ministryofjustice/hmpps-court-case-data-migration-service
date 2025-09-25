package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp

data class Ethnicity(
  val id: Int,
  val observedEthnicityId: Int?,
  val observedEthnicityCode: String?,
  val observedEthnicityDescription: String?,
  val selfDefinedEthnicityId: Int?,
  val selfDefinedEthnicityCode: String?,
  val selfDefinedEthnicityDescription: String?,
  val createdAt: Timestamp?,
  val createdBy: String?,
  val updatedAt: Timestamp?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
