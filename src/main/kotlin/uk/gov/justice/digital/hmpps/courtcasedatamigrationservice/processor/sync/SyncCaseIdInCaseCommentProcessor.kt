package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.sync

import org.springframework.batch.item.ItemProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.CaseComment
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.ProsecutionCase

class SyncCaseIdInCaseCommentProcessor : ItemProcessor<ProsecutionCase, CaseComment> {

  override fun process(prosecutionCase: ProsecutionCase): CaseComment = CaseComment(
    id = null,
    legacyID = null,
    legacyDefendantID = null,
    defendantID = null,
    caseID = prosecutionCase.id,
    legacyCaseID = prosecutionCase.caseID,
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
