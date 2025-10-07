package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp

data class Address(
  val id: Int,
  val address1: String?,
  val address2: String?,
  val address3: String?,
  val address4: String?,
  val address5: String?,
  val postcode: String?,
  val createdAt: Timestamp?,
  val createdBy: String?,
  val updatedAt: Timestamp?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
