package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Timestamp

data class Offender(
  val id: Int,
  val defendantId: Int?,
  val sittingDay: String?,
  val ListingSequence: String?,
  val listingDurationMinutes: String?,
  val isCancelled: String?,
  val suspendedSentenceOrder: String?,
  val breach: String?,
  val awaitingPSR: String?,
  val probationStatus: String?,
  val preSentenceActivity: String?,
  val previouslyKnownTerminationDate: String?,
  val createdAt: Timestamp?,
  val createdBy: String?,
  val updatedAt: Timestamp?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
