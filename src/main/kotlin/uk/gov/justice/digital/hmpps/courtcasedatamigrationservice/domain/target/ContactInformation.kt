package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp

data class ContactInformation(
  val id: Int,
  val homeNumber: Int?,
  val workNumber: Int?,
  val mobileNumber: Int?,
  val primaryEmail: String?,
  val secondaryEmail: String?,
  val fax: Int?,
  val createdAt: Timestamp?,
  val createdBy: String?,
  val updatedAt: Timestamp?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
