package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor

import org.springframework.batch.item.ItemProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Offender
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.OffenderMatch

class SyncOffenderIdInOffenderMatchProcessor : ItemProcessor<Offender, OffenderMatch> {

  override fun process(offender: Offender): OffenderMatch = OffenderMatch(
    id = null,
    legacyID = null,
    offenderID = offender.id,
    legacyOffenderID = offender.legacyID,
    offenderMatchGroupID = null,
    legacyOffenderMatchGroupID = null,
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
