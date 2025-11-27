package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source

import java.math.BigDecimal
import java.sql.Timestamp

data class OffenderMatchQueryResult(
  val id: Int,
  val fkOffenderId: Long?,
  val groupId: Long?,
  val matchType: String?,
  val aliases: String?,
  val rejected: Boolean,
  val matchProbability: BigDecimal?,
  val created: Timestamp?,
  val createdBy: String?,
  val lastUpdated: Timestamp?,
  val lastUpdatedBy: String?,
  val deleted: Boolean?,
  val version: Int?,
)
