package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor

import com.github.f4b6a3.uuid.UuidCreator
import org.springframework.batch.item.ItemProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.OffenderMatchGroupQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.OffenderMatchGroup

class OffenderMatchGroupProcessor : ItemProcessor<OffenderMatchGroupQueryResult, OffenderMatchGroup> {

  override fun process(offenderMatchGroupQueryResult: OffenderMatchGroupQueryResult): OffenderMatchGroup = OffenderMatchGroup(
    id = UuidCreator.getTimeOrderedEpochPlus1(),
    legacyID = offenderMatchGroupQueryResult.id.toLong(),
    defendantID = null,
    legacyDefendantID = offenderMatchGroupQueryResult.defendantId?.toLong(),
    prosecutionCaseID = null,
    legacyProsecutionCaseID = offenderMatchGroupQueryResult.caseId?.toLong(),
    createdAt = offenderMatchGroupQueryResult.created,
    createdBy = offenderMatchGroupQueryResult.createdBy,
    updatedAt = offenderMatchGroupQueryResult.lastUpdated,
    updatedBy = offenderMatchGroupQueryResult.lastUpdatedBy,
    isDeleted = offenderMatchGroupQueryResult.deleted,
    version = offenderMatchGroupQueryResult.version,
  )
}
