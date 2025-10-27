package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.CaseDocument
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.CaseMarker
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.CaseQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.CaseURN
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.CaseURNs
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.ProsecutionCase
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.util.DateUtils.normalizeIsoDateTime
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.CaseDocument as TargetCaseDocument
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.CaseMarker as TargetCaseMarker

class CaseProcessor : ItemProcessor<CaseQueryResult, ProsecutionCase> {

  private companion object {
    val log: Logger = LoggerFactory.getLogger(this::class.java)
  }

  private val objectMapper = jacksonObjectMapper()

  override fun process(caseQueryResult: CaseQueryResult): ProsecutionCase {
//    log.info("Processing case: {}", caseQueryResult.id)

    return ProsecutionCase(
      id = caseQueryResult.id,
      caseId = caseQueryResult.caseId,
      caseURN = buildCaseURNAsJSONBString(caseQueryResult),
      sourceType = caseQueryResult.sourceType,
      cID = null,
      caseMarkers = buildCaseMarkersAsJSONBString(caseQueryResult),
      caseDocuments = buildCaseDocumentsAsJSONBString(caseQueryResult),
      createdAt = caseQueryResult.created,
      createdBy = caseQueryResult.createdBy,
      updatedAt = caseQueryResult.lastUpdated,
      updatedBy = caseQueryResult.lastUpdatedBy,
      isDeleted = caseQueryResult.deleted,
      version = caseQueryResult.version,
    )
  }

  // TODO change this to a list? Check id is not required here
  private fun buildCaseURNAsJSONBString(
    caseQueryResult: CaseQueryResult,
  ): String? {
    if (caseQueryResult.urn == null) return null
    return objectMapper.writeValueAsString(
      CaseURNs(
        caseURNs = listOf(
          CaseURN(
            caseURN = caseQueryResult.urn,
            createdAt = normalizeIsoDateTime(caseQueryResult.created),
            createdBy = caseQueryResult.createdBy,
            updatedAt = normalizeIsoDateTime(caseQueryResult.lastUpdated),
            updatedBy = caseQueryResult.lastUpdatedBy,
            isDeleted = caseQueryResult.deleted,
            version = caseQueryResult.version,
          ),
        ),
      ),
    )
  }

  // TODO do we want to capture createdBy etc for this as its not on the new schema but captured in existing db
  // TODO review the data types in createdBy etc as theyre string and not timestamp
  private fun buildCaseMarkersAsJSONBString(caseQueryResult: CaseQueryResult): String? {
    val caseMarkers: List<TargetCaseMarker>? = caseQueryResult.caseMarkers?.let { json ->
      val results: List<CaseMarker> = objectMapper.readValue(json)
      results.map { result ->
        TargetCaseMarker(
          id = result.id,
          typeId = null,
          typeCode = null,
          typeDescription = result.typeDescription,
          createdAt = normalizeIsoDateTime(result.created),
          createdBy = result.createdBy,
          updatedAt = normalizeIsoDateTime(result.lastUpdated),
          updatedBy = result.lastUpdatedBy,
          isDeleted = result.deleted,
          version = result.version,
        )
      }
    }
    return if (caseMarkers != null) objectMapper.writeValueAsString(caseMarkers) else null
  }

  private fun buildCaseDocumentsAsJSONBString(caseQueryResult: CaseQueryResult): String? {
    val caseDocuments: List<TargetCaseDocument>? = caseQueryResult.caseDocuments?.let { json ->
      val results: List<CaseDocument> = objectMapper.readValue(json)
      results.map { result ->
        TargetCaseDocument(
          id = result.id,
          documentId = result.documentId,
          documentName = result.documentName,
          createdAt = normalizeIsoDateTime(result.created),
          createdBy = result.createdBy,
          updatedAt = normalizeIsoDateTime(result.lastUpdated),
          updatedBy = result.lastUpdatedBy,
          isDeleted = result.deleted,
          version = result.version,
        )
      }
    }
    return if (caseDocuments != null) objectMapper.writeValueAsString(caseDocuments) else null
  }
}
