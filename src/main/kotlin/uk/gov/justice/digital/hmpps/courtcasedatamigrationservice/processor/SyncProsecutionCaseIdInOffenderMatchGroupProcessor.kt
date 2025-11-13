package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor

import org.springframework.batch.item.ItemProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.OffenderMatchGroup
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.ProsecutionCase

class SyncProsecutionCaseIdInOffenderMatchGroupProcessor : ItemProcessor<ProsecutionCase, OffenderMatchGroup> {

  override fun process(prosecutionCase: ProsecutionCase): OffenderMatchGroup = OffenderMatchGroup(
    id = null,
    legacyID = null,
    prosecutionCaseID = prosecutionCase.id,
    legacyProsecutionCaseID = prosecutionCase.legacyID,
    defendantID = null,
    legacyDefendantID = null,
    createdAt = null,
    createdBy = null,
    updatedAt = null,
    updatedBy = null,
    isDeleted = null,
    version = null,
  )
}
