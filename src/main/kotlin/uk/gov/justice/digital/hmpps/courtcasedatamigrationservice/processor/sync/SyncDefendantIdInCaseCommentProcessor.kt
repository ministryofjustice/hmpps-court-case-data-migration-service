package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.sync

import org.springframework.batch.item.ItemProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.CaseComment
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Defendant

class SyncDefendantIdInCaseCommentProcessor : ItemProcessor<Defendant, CaseComment> {

  override fun process(defendant: Defendant): CaseComment = CaseComment(
    id = null,
    legacyID = null,
    legacyDefendantID = defendant.defendantID,
    defendantID = defendant.id,
    caseID = null,
    legacyCaseID = null,
    author = null,
    comment = null,
    isDraft = null,
    isLegacy = null,
    createdAt = null,
    createdBy = null,
    updatedAt = null,
    updatedBy = null,
    isDeleted = null,
    version = null,
  )
}
