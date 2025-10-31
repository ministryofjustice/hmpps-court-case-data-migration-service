package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor

import org.springframework.batch.item.ItemProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.OffenderQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Offender

class OffenderProcessor : ItemProcessor<OffenderQueryResult, Offender> {

  override fun process(offenderQueryResult: OffenderQueryResult): Offender = Offender(
    id = offenderQueryResult.id,
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
