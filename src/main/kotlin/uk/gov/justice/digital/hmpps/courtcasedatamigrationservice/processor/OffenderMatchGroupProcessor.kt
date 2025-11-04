package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor

import org.springframework.batch.item.ItemProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.OffenderMatchGroupQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.OffenderMatchGroup

class OffenderMatchGroupProcessor : ItemProcessor<OffenderMatchGroupQueryResult, OffenderMatchGroup> {

  override fun process(offenderMatchGroupQueryResult: OffenderMatchGroupQueryResult): OffenderMatchGroup {
    // comment to be removed

    return OffenderMatchGroup(
      id = offenderMatchGroupQueryResult.id,
      defendantId = offenderMatchGroupQueryResult.defendantId,
      prosecutionCaseId = offenderMatchGroupQueryResult.caseId,
      createdAt = offenderMatchGroupQueryResult.created,
      createdBy = offenderMatchGroupQueryResult.createdBy,
      updatedAt = offenderMatchGroupQueryResult.lastUpdated,
      updatedBy = offenderMatchGroupQueryResult.lastUpdatedBy,
      isDeleted = offenderMatchGroupQueryResult.deleted,
      version = offenderMatchGroupQueryResult.version,
    )
  }
}
