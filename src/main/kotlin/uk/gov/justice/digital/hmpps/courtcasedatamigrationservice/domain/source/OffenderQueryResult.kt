package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source

import java.sql.Date
import java.sql.Timestamp

data class OffenderQueryResult(
  val id: Int,
  val suspendedSentenceOrder: Boolean?,
  val breach: Boolean?,
  val awaitingPSR: Boolean?,
  val probationStatus: String?,
  val preSentenceActivity: Boolean?,
  val previouslyKnownTerminationDate: Date?,
  val created: Timestamp?,
  val createdBy: String?,
  val lastUpdated: Timestamp?,
  val lastUpdatedBy: String?,
  val deleted: Boolean?,
  val version: Int?,
)
