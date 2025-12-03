package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.config.job

import org.slf4j.LoggerFactory
import org.springframework.batch.core.Job
import org.springframework.batch.core.SkipListener
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.batch.core.launch.support.RunIdIncrementer
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.core.step.tasklet.Tasklet
import org.springframework.batch.item.ItemProcessor
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider
import org.springframework.batch.item.database.JdbcBatchItemWriter
import org.springframework.batch.item.database.JdbcCursorItemReader
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder
import org.springframework.batch.item.support.builder.CompositeItemProcessorBuilder
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.transaction.PlatformTransactionManager
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.config.BatchProperties
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.CaseCommentConstants.MAX_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.CaseCommentConstants.MIN_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.CaseCommentConstants.SOURCE_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.CaseCommentConstants.SOURCE_ROW_COUNT_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.CaseCommentConstants.SYNC_DEFENDANT_ID_MAX_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.CaseCommentConstants.SYNC_DEFENDANT_ID_MIN_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.CaseCommentConstants.SYNC_DEFENDANT_ID_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.CaseCommentConstants.TARGET_ROW_COUNT_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.JobType
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.SyncJobType
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.CaseCommentQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.CaseComment
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Defendant
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.RowCountListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.TimerJobListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.CaseCommentProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.sync.SyncDefendantIdInCaseCommentProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler.JobScheduler
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler.SchedulingConfigRepository
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler.SyncJobScheduler
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service.JobService
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.CaseCommentValidator
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.PostMigrationValidator
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.sync.DefendantFkInCaseCommentValidator
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.sync.SyncPostMigrationValidator
import java.util.UUID
import javax.sql.DataSource

@Configuration
@EnableBatchProcessing
class CaseCommentBatchConfig(
  private val jobRepository: JobRepository,
  private val transactionManager: PlatformTransactionManager,
  @Qualifier("sourceDataSource") private val sourceDataSource: DataSource,
  @Qualifier("targetDataSource") private val targetDataSource: DataSource,
  private val batchProperties: BatchProperties,
) {

  private val log = LoggerFactory.getLogger(CaseCommentBatchConfig::class.java)

  @Autowired
  lateinit var jobLauncher: JobLauncher

  @Bean
  @StepScope
  fun caseCommentReader(
    @Value("#{jobParameters['minId']}") minId: Long?,
    @Value("#{jobParameters['maxId']}") maxId: Long?,
  ): JdbcCursorItemReader<CaseCommentQueryResult> = JdbcCursorItemReaderBuilder<CaseCommentQueryResult>()
    .name("caseCommentReader")
    .dataSource(sourceDataSource)
    .fetchSize(3000)
    .sql("${SOURCE_QUERY} WHERE cc.id BETWEEN $minId AND $maxId order by cc.id asc")
    .rowMapper { rs, _ ->
      CaseCommentQueryResult(
        id = rs.getInt("id"),
        caseID = rs.getString("case_id"),
        defendantID = rs.getObject("defendant_id", UUID::class.java),
        author = rs.getString("author"),
        comment = rs.getString("comment"),
        isDraft = rs.getBoolean("is_draft"),
        legacy = rs.getBoolean("legacy"),
        created = rs.getTimestamp("created"),
        createdBy = rs.getString("created_by"),
        lastUpdated = rs.getTimestamp("last_updated"),
        lastUpdatedBy = rs.getString("last_updated_by"),
        deleted = rs.getBoolean("deleted"),
        version = rs.getInt("version"),
      )
    }
    .build()

  @Bean
  fun caseCommentProcessor(): ItemProcessor<CaseCommentQueryResult, CaseComment> = CompositeItemProcessorBuilder<CaseCommentQueryResult, CaseComment>()
    .delegates(listOf(CaseCommentProcessor()))
    .build()

  @Bean
  fun caseCommentWriter(): JdbcBatchItemWriter<CaseComment> = JdbcBatchItemWriterBuilder<CaseComment>()
    .itemSqlParameterSourceProvider(BeanPropertyItemSqlParameterSourceProvider())
    .sql(
      """INSERT INTO hmpps_court_case_service.case_comment (id, legacy_id, defendant_id, legacy_defendant_id, case_id, legacy_case_id, author, comment, is_draft, is_legacy, created_at, created_by, updated_at, updated_by, is_deleted, version)
        VALUES (:id, :legacyID, :defendantID, :legacyDefendantID, :caseID, :legacyCaseID, :author, :comment, :isDraft, :isLegacy, :createdAt, :createdBy, :updatedAt, :updatedBy, :isDeleted, :version)""",
    )
    .dataSource(targetDataSource)
    .build()

  @Bean
  fun caseCommentSkipListener() = object : SkipListener<CaseCommentQueryResult, CaseComment> {
    override fun onSkipInRead(t: Throwable) {
      log.warn("Skipped during read: ${t.message}")
    }

    override fun onSkipInProcess(item: CaseCommentQueryResult, t: Throwable) {
      log.warn("Skipped during process: ${item.id}, reason: ${t.message}")
    }

    override fun onSkipInWrite(item: CaseComment, t: Throwable) {
      log.warn("Skipped during write: ${item.id}, reason: ${t.message}")
    }
  }

  @Bean
  fun caseCommentStep(): Step = StepBuilder("caseCommentStep", jobRepository)
    .chunk<CaseCommentQueryResult, CaseComment>(batchProperties.chunkSize, transactionManager)
    .reader(caseCommentReader(null, null))
    .processor(caseCommentProcessor())
    .writer(caseCommentWriter())
    .listener(caseCommentSkipListener())
    .faultTolerant()
    .retry(Throwable::class.java)
    .retryLimit(3)
    .build()

  @Bean
  fun caseCommentRowCountListener(): RowCountListener = RowCountListener(
    sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
    targetJdbcTemplate = JdbcTemplate(targetDataSource),
    sourceRowCountQuery = SOURCE_ROW_COUNT_QUERY,
    targetRowCountQuery = TARGET_ROW_COUNT_QUERY,
  )

  fun validationStep(): Step = StepBuilder("validationStep", jobRepository)
    .tasklet(validationTasklet(), transactionManager)
    .build()

  fun validationTasklet(): Tasklet {
    val strategy = CaseCommentValidator(
      sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
      targetJdbcTemplate = JdbcTemplate(targetDataSource),
    )
    return PostMigrationValidator(strategy, 100)
  }

  @Bean
  fun caseCommentJob(timerJobListener: TimerJobListener): Job = JobBuilder("caseCommentJob", jobRepository)
    .incrementer(RunIdIncrementer())
    .listener(timerJobListener)
    .listener(caseCommentRowCountListener())
    .start(caseCommentStep())
    .next(validationStep())
    .build()

  @Bean(name = ["caseCommentJobService"])
  fun caseCommentJobService(@Qualifier("caseCommentJob") caseCommentJob: Job): JobService = JobService(
    jobLauncher = jobLauncher,
    job = caseCommentJob,
    jdbcTemplate = JdbcTemplate(sourceDataSource),
    batchSize = 15,
    minQuery = MIN_QUERY,
    maxQuery = MAX_QUERY,
    jobName = "CaseComment",
  )

  @Bean
  fun caseCommentJobScheduler(dataSource: DataSource, timerJobListener: TimerJobListener) = JobScheduler(
    jobService = caseCommentJobService(caseCommentJob(timerJobListener)),
    jobType = JobType.CASE_COMMENT,
    schedulingConfigRepository = SchedulingConfigRepository(JdbcTemplate(dataSource)),
  )

  /**
   *
   *
   * Update DefendantID FK job below.
   *
   *
   *
   */

  @Bean(name = ["syncDefendantIdInCaseCommentJobService"])
  fun syncDefendantIdInCaseCommentJobService(@Qualifier("syncDefendantIdInCaseCommentJob") syncDefendantIdInCaseCommentJob: Job): JobService = JobService(
    jobLauncher = jobLauncher,
    job = syncDefendantIdInCaseCommentJob,
    jdbcTemplate = JdbcTemplate(targetDataSource),
    batchSize = 15,
    minQuery = SYNC_DEFENDANT_ID_MIN_QUERY,
    maxQuery = SYNC_DEFENDANT_ID_MAX_QUERY,
    jobName = "syncDefendantIdInCaseComment",
  )

  fun defendantFkInCaseCommentValidationStep(): Step = StepBuilder("defendantFkInCaseCommentValidationStep", jobRepository)
    .tasklet(defendantFkInCaseCommentValidationTasklet(), transactionManager)
    .build()

  fun defendantFkInCaseCommentValidationTasklet(): Tasklet {
    val strategy = DefendantFkInCaseCommentValidator(
      sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
      targetJdbcTemplate = JdbcTemplate(targetDataSource),
    )
    return SyncPostMigrationValidator(strategy, 100)
  }

  @Bean
  fun syncDefendantIdInCaseCommentJob(timerJobListener: TimerJobListener): Job = JobBuilder("syncDefendantIdInCaseCommentJob", jobRepository)
    .incrementer(RunIdIncrementer())
    .listener(timerJobListener)
    .start(syncDefendantIdInCaseCommentStep())
    .next(defendantFkInCaseCommentValidationStep())
    .build()

  @Bean
  @StepScope
  fun syncDefendantIdInCaseCommentReader(
    @Value("#{jobParameters['minId']}") minId: Long?,
    @Value("#{jobParameters['maxId']}") maxId: Long?,
  ): JdbcCursorItemReader<Defendant> = JdbcCursorItemReaderBuilder<Defendant>()
    .name("syncDefendantIdInCaseCommentReader")
    .dataSource(targetDataSource)
    .fetchSize(3000)
    .sql("${SYNC_DEFENDANT_ID_QUERY} WHERE d.legacy_id BETWEEN $minId AND $maxId ORDER BY d.legacy_id ASC")
    .rowMapper { rs, _ ->
      Defendant(
        id = rs.getObject("id", UUID::class.java),
        legacyID = null,
        defendantID = rs.getObject("defendant_id", UUID::class.java),
        isManualUpdate = null,
        crn = null,
        croNumber = null,
        tsvName = null,
        pncId = null,
        cprUUID = null,
        isOffenderConfirmed = null,
        person = null,
        legacyOffenderID = null,
        offenderID = null,
        createdAt = null,
        createdBy = null,
        updatedAt = null,
        updatedBy = null,
        isDeleted = null,
        version = null,
      )
    }
    .build()

  @Bean
  fun syncDefendantIdInCaseCommentProcessor(): ItemProcessor<Defendant, CaseComment> = CompositeItemProcessorBuilder<Defendant, CaseComment>()
    .delegates(listOf(SyncDefendantIdInCaseCommentProcessor()))
    .build()

  @Bean
  fun syncDefendantIdInCaseCommentWriter(): JdbcBatchItemWriter<CaseComment> = JdbcBatchItemWriterBuilder<CaseComment>()
    .itemSqlParameterSourceProvider(BeanPropertyItemSqlParameterSourceProvider())
    .sql(
      """UPDATE hmpps_court_case_service.case_comment SET defendant_id = :defendantID WHERE legacy_defendant_id = :legacyDefendantID""",
    )
    .dataSource(targetDataSource)
    .assertUpdates(false)
    .build()

  @Bean
  fun syncDefendantIdInCaseCommentStep(): Step = StepBuilder("syncDefendantIdInCaseCommentStep", jobRepository)
    .chunk<Defendant, CaseComment>(batchProperties.chunkSize, transactionManager)
    .reader(syncDefendantIdInCaseCommentReader(null, null))
    .processor(syncDefendantIdInCaseCommentProcessor())
    .writer(syncDefendantIdInCaseCommentWriter())
    .faultTolerant()
    .retry(Throwable::class.java)
    .retryLimit(3)
    .build()

  @Bean
  fun syncDefendantIdInCaseCommentJobScheduler(dataSource: DataSource, timerJobListener: TimerJobListener) = SyncJobScheduler(
    jobService = syncDefendantIdInCaseCommentJobService(syncDefendantIdInCaseCommentJob(timerJobListener)),
    syncJobType = SyncJobType.DEFENDANT_ID_CASE_COMMENT,
    schedulingConfigRepository = SchedulingConfigRepository(JdbcTemplate(dataSource)),
  )
}
