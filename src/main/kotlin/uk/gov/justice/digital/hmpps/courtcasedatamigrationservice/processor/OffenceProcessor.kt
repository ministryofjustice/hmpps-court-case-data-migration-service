package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.github.f4b6a3.uuid.UuidCreator
import org.springframework.batch.item.ItemProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.JudicialResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.OffenceQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Offence
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Plea
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Verdict
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.util.DateUtils.normalizeIsoDateTime
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.JudicialResult as TargetJudicialResult

class OffenceProcessor : ItemProcessor<OffenceQueryResult, Offence> {

  private val objectMapper = jacksonObjectMapper()

  override fun process(offenceQueryResult: OffenceQueryResult): Offence {
    val plea = if (offenceQueryResult.pleaId != null) buildPleaAsJSONBString(offenceQueryResult.pleaId, offenceQueryResult) else null
    val verdict = if (offenceQueryResult.verdictId != null) buildVerdictAsJSONBString(offenceQueryResult.verdictId, offenceQueryResult) else null

    val offence = Offence(
      id = UuidCreator.getTimeOrderedEpochPlus1(),
      legacyID = offenceQueryResult.id.toLong(),
      title = offenceQueryResult.title,
      legislation = offenceQueryResult.act,
      code = offenceQueryResult.offenceCode,
      listingNumber = offenceQueryResult.listNo,
      sequence = offenceQueryResult.sequence,
      shortTermCustodyPredictorScore = offenceQueryResult.shortTermCustodyPredictorScore,
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
          id = UuidCreator.getTimeOrderedEpochPlus1(),
          legacyID = result.id.toLong(),
          label = result.label,
          isConvictedResult = result.isConvictedResult,
          resultTypeID = result.judicialResultTypeId,
          isJudicialResultDeleted = null, // not at source so not included in migrated data.
          resultText = result.resultText,
          createdAt = normalizeIsoDateTime(result.created),
          createdBy = result.createdBy,
          updatedAt = normalizeIsoDateTime(result.lastUpdated),
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
      id = UuidCreator.getTimeOrderedEpochPlus1(),
      legacyID = verdictId.toLong(),
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
      id = UuidCreator.getTimeOrderedEpochPlus1(),
      legacyID = pleaId.toLong(),
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
