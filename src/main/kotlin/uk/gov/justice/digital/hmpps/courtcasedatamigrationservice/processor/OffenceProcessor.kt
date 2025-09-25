package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.OffenceQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Offence
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Plea
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Verdict

class OffenceProcessor : ItemProcessor<OffenceQueryResult, Offence> {

  private companion object {
    val log: Logger = LoggerFactory.getLogger(this::class.java)
  }

  private val objectMapper = jacksonObjectMapper()

  override fun process(offenceQueryResult: OffenceQueryResult): Offence {
    log.info("Processing offence: {}", offenceQueryResult.id)

    val plea = if (offenceQueryResult.pleaId != null) buildPleaAsJSONBString(offenceQueryResult.pleaId, offenceQueryResult) else null
    val verdict = if (offenceQueryResult.verdictId != null) buildVerdictAsJSONBString(offenceQueryResult.verdictId, offenceQueryResult) else null

    val offence = Offence(
      id = offenceQueryResult.id,
      title = offenceQueryResult.title,
      act = offenceQueryResult.act,
      code = offenceQueryResult.offenceCode,
      list_number = offenceQueryResult.listNo,
      sequence = offenceQueryResult.sequence,
      facts = null,
      isDiscontinued = false,
      shortTermCustodyPredictorScore = offenceQueryResult.shortTeamCustodyPredictorScore,
      judicialResult = null,
      plea = plea,
      verdict = verdict,
      createdAt = offenceQueryResult.created,
      createdBy = offenceQueryResult.createdBy,
      updatedAt = offenceQueryResult.lastUpdated,
      updatedBy = offenceQueryResult.lastUpdatedBy,
      isDeleted = offenceQueryResult.deleted,
      version = offenceQueryResult.version,
    )

    return offence
  }

  private fun buildVerdictAsJSONBString(
    verdictId: Int,
    offenceQueryResult: OffenceQueryResult,
  ): String? = objectMapper.writeValueAsString(
    Verdict(
      id = verdictId,
      date = offenceQueryResult.verdictDate,
      type = offenceQueryResult.verdictTypeDescription,
      createdAt = offenceQueryResult.verdictCreated,
      createdBy = offenceQueryResult.verdictCreatedBy,
      lastUpdatedAt = offenceQueryResult.verdictLastUpdated,
      lastUpdatedBy = offenceQueryResult.verdictLastUpdatedBy,
      isDeleted = offenceQueryResult.verdictDeleted,
      version = offenceQueryResult.verdictVersion,
    ),
  )

  private fun buildPleaAsJSONBString(
    pleaId: Int,
    offenceQueryResult: OffenceQueryResult,
  ): String? = objectMapper.writeValueAsString(
    Plea(
      id = pleaId,
      date = offenceQueryResult.pleaDate,
      value = offenceQueryResult.pleaValue,
      createdAt = offenceQueryResult.pleaCreated,
      createdBy = offenceQueryResult.pleaCreatedBy,
      lastUpdatedAt = offenceQueryResult.pleaLastUpdated,
      lastUpdatedBy = offenceQueryResult.pleaLastUpdatedBy,
      isDeleted = offenceQueryResult.pleaDeleted,
      version = offenceQueryResult.pleaVersion,
    ),
  )
}
