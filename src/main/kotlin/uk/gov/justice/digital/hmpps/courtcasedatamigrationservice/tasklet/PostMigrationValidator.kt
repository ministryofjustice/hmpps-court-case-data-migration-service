package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet

import org.slf4j.LoggerFactory
import org.springframework.batch.core.StepContribution
import org.springframework.batch.core.scope.context.ChunkContext
import org.springframework.batch.core.step.tasklet.Tasklet
import org.springframework.batch.repeat.RepeatStatus
import kotlin.random.Random

class PostMigrationValidator(
  private val strategy: PostMigrationValidationStrategy,
  private val sampleSize: Int = 10,
) : Tasklet {

  private val log = LoggerFactory.getLogger(PostMigrationValidator::class.java)

  override fun execute(
    contribution: StepContribution,
    chunkContext: ChunkContext,
  ): RepeatStatus = validateSample(chunkContext.stepContext.jobParameters)

  fun validateSample(jobParameters: Map<String, Any>): RepeatStatus {
    log.info("Validating sample size {}", sampleSize)

    val minId = jobParameters["minId"] as? Long
    val maxId = jobParameters["maxId"] as? Long

    if (minId == null || maxId == null || minId > maxId) {
      throw RuntimeException("Invalid ID range. Stopping further job executions.")
    }

    val presentSourceIds = mutableListOf<Long>()
    val attemptedIds = mutableSetOf<Long>()

    while (presentSourceIds.size < sampleSize && attemptedIds.size < (maxId - minId)) {
      val id = Random.nextLong(minId, maxId + 1)
      if (id in attemptedIds) continue
      attemptedIds.add(id)

      val source = strategy.fetchSourceRecord(id)
      if (source != null) {
        presentSourceIds.add(id)
      }
    }

    log.info("Validating Sample Source IDs: {}", presentSourceIds.joinToString(", "))

    val errors = mutableListOf<String>()

    presentSourceIds.forEach { id ->
      val source = strategy.fetchSourceRecord(id)
      val target = strategy.fetchTargetRecord(id)

      if (source == null || target == null) {
        errors.add("Missing record for ID $id")
        return@forEach
      }

      errors.addAll(strategy.compareRecords(source, target))
    }

    if (errors.isEmpty()) {
      log.info("✅ All sample records passed validation.")
    } else {
      log.error("❌ Validation errors found:\n${errors.joinToString("\n")}. Stopping further job executions.")
      throw RuntimeException("Validation failed for job with parameters: $jobParameters")
    }

    return RepeatStatus.FINISHED
  }
}
