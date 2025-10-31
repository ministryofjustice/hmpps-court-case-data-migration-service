package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source

import java.sql.Timestamp
import java.time.LocalDate

class DefendantQueryResult(
  val id: Int,
  val isManualUpdate: Boolean?,
  val crn: String?,
  val cro: String?,
  val name: String?,
  val dateOfBirth: LocalDate?,
  val offenderConfirmed: Boolean?,
  val nationality1: String?,
  val nationality2: String?,
  val sex: String?,
  val phoneNumber: String?,
  val address: String?,
  val tsvName: String?,
  val pnc: String?,
  val cprUUID: String?,
  val fkOffenderId: Long?,
  val created: Timestamp?,
  val createdBy: String?,
  val lastUpdated: Timestamp?,
  val lastUpdatedBy: String?,
  val deleted: Boolean?,
  val version: Int?,
)
