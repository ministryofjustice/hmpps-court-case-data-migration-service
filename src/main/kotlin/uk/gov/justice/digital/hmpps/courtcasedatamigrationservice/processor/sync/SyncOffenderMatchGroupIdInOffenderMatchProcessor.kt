package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.sync

import org.springframework.batch.item.ItemProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.OffenderMatch
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.OffenderMatchGroup

class SyncOffenderMatchGroupIdInOffenderMatchProcessor : ItemProcessor<OffenderMatchGroup, OffenderMatch> {

  override fun process(offenderMatchGroup: OffenderMatchGroup): OffenderMatch = OffenderMatch(
    id = null,
    legacyID = null,
    offenderID = null,
    legacyOffenderID = null,
    offenderMatchGroupID = offenderMatchGroup.id,
    legacyOffenderMatchGroupID = offenderMatchGroup.legacyID,
    matchType = null,
    aliases = null,
    isRejected = null,
    matchProbability = null,
    createdAt = null,
    createdBy = null,
    updatedAt = null,
    updatedBy = null,
    isDeleted = null,
    version = null,
  )
}
