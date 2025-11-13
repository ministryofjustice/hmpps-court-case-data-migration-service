package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target

import java.sql.Date
import java.sql.Timestamp
import java.util.UUID

data class Offender(
  val id: UUID,
  val legacyID: Long?,
  val suspendedSentenceOrder: Boolean?,
  val breach: Boolean?,
  val awaitingPSR: Boolean?,
  val probationStatus: String?,
  val preSentenceActivity: Boolean?,
  val previouslyKnownTerminationDate: Date?,
  val createdAt: Timestamp?,
  val createdBy: String?,
  val updatedAt: Timestamp?,
  val updatedBy: String?,
  val isDeleted: Boolean?,
  val version: Int?,
)
