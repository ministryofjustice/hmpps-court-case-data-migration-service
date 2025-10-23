package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp

data class ContactInformation(
  val id: Int,
  val homeNumber: String?,
  val workNumber: String?,
  val mobileNumber: String?,
  val primaryEmail: String?,
  val secondaryEmail: String?,
  val fax: Int?,
  val createdAt: String?,
  val createdBy: String?,
  val updatedAt: String?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
