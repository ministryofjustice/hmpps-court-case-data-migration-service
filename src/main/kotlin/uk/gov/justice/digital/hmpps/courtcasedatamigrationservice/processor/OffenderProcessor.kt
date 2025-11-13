package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor

import com.github.f4b6a3.uuid.UuidCreator
import org.springframework.batch.item.ItemProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.OffenderQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Offender

class OffenderProcessor : ItemProcessor<OffenderQueryResult, Offender> {

  override fun process(offenderQueryResult: OffenderQueryResult): Offender = Offender(
    id = UuidCreator.getTimeOrderedEpochPlus1(),
    legacyID = offenderQueryResult.id.toLong(),
    suspendedSentenceOrder = offenderQueryResult.suspendedSentenceOrder,
    breach = offenderQueryResult.breach,
    awaitingPSR = offenderQueryResult.awaitingPSR,
    probationStatus = offenderQueryResult.probationStatus,
    preSentenceActivity = offenderQueryResult.preSentenceActivity,
    previouslyKnownTerminationDate = offenderQueryResult.previouslyKnownTerminationDate,
    createdAt = offenderQueryResult.created,
    createdBy = offenderQueryResult.createdBy,
    updatedAt = offenderQueryResult.lastUpdated,
    updatedBy = offenderQueryResult.lastUpdatedBy,
    isDeleted = offenderQueryResult.deleted,
    version = offenderQueryResult.version,
  )
}
