package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.github.f4b6a3.uuid.UuidCreator
import org.springframework.batch.item.ItemProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.HearingNote
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.HearingOutcome
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.HearingQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Hearing
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.HearingCaseNote
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.util.DateUtils.normalizeIsoDateTime
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.HearingOutcome as TargetHearingOutcome

class HearingProcessor : ItemProcessor<HearingQueryResult, Hearing> {

  private val objectMapper = jacksonObjectMapper().apply {
    registerModule(JavaTimeModule())
  }

  override fun process(hearingQueryResult: HearingQueryResult): Hearing {
    val hearing = Hearing(
      id = UuidCreator.getTimeOrderedEpochPlus1(),
      legacyID = hearingQueryResult.id.toLong(),
      type = hearingQueryResult.hearingType,
      eventType = hearingQueryResult.hearingEventType,
      listNumber = hearingQueryResult.listNo,
      firstCreated = hearingQueryResult.firstCreated,
      hearingOutcome = buildHearingOutcomeAsJSONBString(hearingQueryResult),
      hearingCaseNote = buildHearingCaseNotesAsJSONBString(hearingQueryResult),
      createdAt = hearingQueryResult.created,
      createdBy = hearingQueryResult.createdBy,
      updatedAt = hearingQueryResult.lastUpdated,
      updatedBy = hearingQueryResult.lastUpdatedBy,
      isDeleted = hearingQueryResult.deleted,
      version = hearingQueryResult.version,
    )

    return hearing
  }

  private fun buildHearingOutcomeAsJSONBString(hearingQueryResult: HearingQueryResult): String? {
    val hearingOutcomes: List<TargetHearingOutcome>? = hearingQueryResult.hearingOutcomes?.let { json ->
      val results: List<HearingOutcome> = objectMapper.readValue(json)
      results.map { result ->
        TargetHearingOutcome(
          id = result.id,
          defendantID = result.defendantId,
          type = result.outcomeType,
          outcomeDate = normalizeIsoDateTime(result.outcomeDate),
          state = result.state,
          assignedTo = result.assignedTo,
          assignedToUUID = result.assignedToUuid,
          resultedDate = normalizeIsoDateTime(result.resultedDate),
          isLegacy = result.legacy,
          createdAt = normalizeIsoDateTime(result.created),
          createdBy = result.createdBy,
          updatedAt = normalizeIsoDateTime(result.lastUpdated),
          updatedBy = result.lastUpdatedBy,
          isDeleted = result.deleted,
          version = result.version,
        )
      }
    }
    return if (hearingOutcomes != null) objectMapper.writeValueAsString(hearingOutcomes) else null
  }

  private fun buildHearingCaseNotesAsJSONBString(hearingQueryResult: HearingQueryResult): String? {
    val hearingCaseNotes: List<HearingCaseNote>? = hearingQueryResult.hearingNotes?.let { json ->
      val results: List<HearingNote> = objectMapper.readValue(json)
      results.map { result ->
        HearingCaseNote(
          id = result.id,
          defendantID = result.defendantId,
          note = result.note,
          author = result.author,
          isDraft = result.draft,
          createdByUUID = result.createdByUuid,
          isLegacy = result.legacy,
          createdAt = normalizeIsoDateTime(result.created),
          createdBy = result.createdBy,
          updatedAt = normalizeIsoDateTime(result.lastUpdated),
          updatedBy = result.lastUpdatedBy,
          isDeleted = result.deleted,
          version = result.version,

        )
      }
    }
    return if (hearingCaseNotes != null) objectMapper.writeValueAsString(hearingCaseNotes) else null
  }
}
