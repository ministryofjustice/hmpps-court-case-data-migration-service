package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.sync

import org.springframework.batch.item.ItemProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.DefendantOffence
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Offence

class SyncOffenceIdInDefendantOffenceProcessor : ItemProcessor<Offence, DefendantOffence> {

  override fun process(offence: Offence): DefendantOffence = DefendantOffence(
    id = null,
    defendantID = null,
    offenceID = offence.id,
    legacyOffenceID = offence.legacyID,
    legacyDefendantID = null,
    createdAt = null,
    createdBy = null,
    updatedAt = null,
    updatedBy = null,
    isDeleted = null,
    version = null,
  )
}
