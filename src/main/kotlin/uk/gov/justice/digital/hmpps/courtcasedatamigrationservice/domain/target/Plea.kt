package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp

data class Plea(
  val id: Int,
  val date: Timestamp?,
  val value: String?,
  val createdAt: Timestamp?,
  val createdBy: String?,
  val lastUpdatedAt: Timestamp?,
  val lastUpdatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
