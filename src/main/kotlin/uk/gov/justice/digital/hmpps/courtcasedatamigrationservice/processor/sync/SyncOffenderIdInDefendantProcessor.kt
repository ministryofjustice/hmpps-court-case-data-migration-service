package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.sync

import org.springframework.batch.item.ItemProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Defendant
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Offender

class SyncOffenderIdInDefendantProcessor : ItemProcessor<Offender, Defendant> {

  override fun process(offender: Offender): Defendant = Defendant(
    id = null,
    defendantID = null,
    legacyID = null,
    isManualUpdate = null,
    crn = null,
    croNumber = null,
    tsvName = null,
    pncId = null,
    cprUUID = null,
    isOffenderConfirmed = null,
    person = null,
    legacyOffenderID = offender.legacyID,
    offenderID = offender.id,
    createdAt = null,
    createdBy = null,
    updatedAt = null,
    updatedBy = null,
    isDeleted = null,
    version = null,
  )
}
