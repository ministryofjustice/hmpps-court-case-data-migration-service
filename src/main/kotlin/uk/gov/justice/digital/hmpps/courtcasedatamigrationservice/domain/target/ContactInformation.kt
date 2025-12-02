package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.util.UUID

data class ContactInformation(
  val id: UUID,
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
