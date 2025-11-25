package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.sync

import org.springframework.batch.item.ItemProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Defendant
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.DefendantOffence

class SyncDefendantIdInDefendantOffenceProcessor : ItemProcessor<Defendant, DefendantOffence> {

  override fun process(defendant: Defendant): DefendantOffence = DefendantOffence(
    id = null,
    defendantID = defendant.id,
    offenceID = null,
    legacyOffenceID = null,
    legacyDefendantID = defendant.legacyID,
    createdAt = null,
    createdBy = null,
    updatedAt = null,
    updatedBy = null,
    isDeleted = null,
    version = null,
  )
}
