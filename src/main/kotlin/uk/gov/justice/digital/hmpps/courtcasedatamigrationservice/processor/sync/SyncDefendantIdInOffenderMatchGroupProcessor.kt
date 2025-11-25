package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.sync

import org.springframework.batch.item.ItemProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Defendant
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.OffenderMatchGroup

class SyncDefendantIdInOffenderMatchGroupProcessor : ItemProcessor<Defendant, OffenderMatchGroup> {

  override fun process(defendant: Defendant): OffenderMatchGroup = OffenderMatchGroup(
    id = null,
    legacyID = null,
    prosecutionCaseID = null,
    legacyProsecutionCaseID = null,
    defendantID = defendant.id,
    legacyDefendantID = defendant.legacyID,
    createdAt = null,
    createdBy = null,
    updatedAt = null,
    updatedBy = null,
    isDeleted = null,
    version = null,
  )
}
