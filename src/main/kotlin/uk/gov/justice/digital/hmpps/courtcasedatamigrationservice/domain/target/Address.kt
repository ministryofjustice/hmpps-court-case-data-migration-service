package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.util.UUID

data class Address(
  val id: UUID,
  val address1: String?,
  val address2: String?,
  val address3: String?,
  val address4: String?,
  val address5: String?,
  val postcode: String?,
  val createdAt: String?,
  val createdBy: String?,
  val updatedAt: String?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
