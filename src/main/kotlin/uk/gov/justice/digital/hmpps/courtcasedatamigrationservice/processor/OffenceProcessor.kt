package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.JudicialResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.OffenceQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Offence
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Plea
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Verdict
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.util.DateUtils.normalizeIsoDateTime
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.JudicialResult as TargetJudicialResult

class OffenceProcessor : ItemProcessor<OffenceQueryResult, Offence> {

  private companion object {
    val log: Logger = LoggerFactory.getLogger(this::class.java)
  }

  private val objectMapper = jacksonObjectMapper()

  override fun process(offenceQueryResult: OffenceQueryResult): Offence {
//    log.info("Processing offence: {}", offenceQueryResult.id)

    val plea = if (offenceQueryResult.pleaId != null) buildPleaAsJSONBString(offenceQueryResult.pleaId, offenceQueryResult) else null
    val verdict = if (offenceQueryResult.verdictId != null) buildVerdictAsJSONBString(offenceQueryResult.verdictId, offenceQueryResult) else null

    val offence = Offence(
      id = offenceQueryResult.id,
      title = offenceQueryResult.title,
      legislation = offenceQueryResult.act,
      code = offenceQueryResult.offenceCode,
      listingNumber = offenceQueryResult.listNo,
      sequence = offenceQueryResult.sequence,
      shortTermCustodyPredictorScore = offenceQueryResult.shortTeamCustodyPredictorScore, // TODO null at source is being saved as 0 in target
      wording = offenceQueryResult.summary,
      judicialResults = buildJudicialResultAsJSONBString(offenceQueryResult),
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

  private fun buildJudicialResultAsJSONBString(offenceQueryResult: OffenceQueryResult): String? {
    val judicialResults: List<TargetJudicialResult>? = offenceQueryResult.judicialResults?.let { json ->
      val results: List<JudicialResult> = objectMapper.readValue(json)
      results.map { result ->
        TargetJudicialResult(
          id = result.id,
          label = result.label,
          isConvictedResult = result.isConvictedResult,
          resultTypeId = result.judicialResultTypeId,
          isJudicialResultDeleted = null, // TODO check where this data comes from
          resultText = result.resultText,
          createdAt = result.created, // TODO review the code here for timestamp
          createdBy = result.createdBy,
          updatedAt = result.lastUpdated,
          updatedBy = result.lastUpdatedBy,
          isDeleted = result.deleted,
          version = result.version,
        )
      }
    }
    return if (judicialResults != null) objectMapper.writeValueAsString(judicialResults) else null
  }

  private fun buildVerdictAsJSONBString(
    verdictId: Int,
    offenceQueryResult: OffenceQueryResult,
  ): String? = objectMapper.writeValueAsString(
    Verdict(
      id = verdictId,
      date = normalizeIsoDateTime(offenceQueryResult.verdictDate),
      type = offenceQueryResult.verdictTypeDescription,
      createdAt = normalizeIsoDateTime(offenceQueryResult.verdictCreated),
      createdBy = offenceQueryResult.verdictCreatedBy,
      updatedAt = normalizeIsoDateTime(offenceQueryResult.verdictLastUpdated),
      updatedBy = offenceQueryResult.verdictLastUpdatedBy,
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
      date = normalizeIsoDateTime(offenceQueryResult.pleaDate),
      value = offenceQueryResult.pleaValue,
      createdAt = normalizeIsoDateTime(offenceQueryResult.pleaCreated),
      createdBy = offenceQueryResult.pleaCreatedBy,
      updatedAt = normalizeIsoDateTime(offenceQueryResult.pleaLastUpdated),
      updatedBy = offenceQueryResult.pleaLastUpdatedBy,
      isDeleted = offenceQueryResult.pleaDeleted,
      version = offenceQueryResult.pleaVersion,
    ),
  )
}
