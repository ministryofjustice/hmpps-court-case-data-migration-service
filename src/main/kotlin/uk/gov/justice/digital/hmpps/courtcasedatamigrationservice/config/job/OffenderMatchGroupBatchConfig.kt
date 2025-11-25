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
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchGroupConstants.MAX_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchGroupConstants.MIN_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchGroupConstants.SOURCE_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchGroupConstants.SOURCE_ROW_COUNT_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchGroupConstants.SYNC_DEFENDANT_ID_MAX_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchGroupConstants.SYNC_DEFENDANT_ID_MIN_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchGroupConstants.SYNC_DEFENDANT_ID_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchGroupConstants.SYNC_PROSECUTION_CASE_ID_MAX_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchGroupConstants.SYNC_PROSECUTION_CASE_ID_MIN_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchGroupConstants.SYNC_PROSECUTION_CASE_ID_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchGroupConstants.TARGET_ROW_COUNT_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.JobType
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.OffenderMatchGroupQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Defendant
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.OffenderMatchGroup
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.ProsecutionCase
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.RowCountListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.TimerJobListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.OffenderMatchGroupProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.sync.SyncDefendantIdInOffenderMatchGroupProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.sync.SyncProsecutionCaseIdInOffenderMatchGroupProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler.JobScheduler
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler.SchedulingConfigRepository
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service.JobService
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.OffenderMatchGroupValidator
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.PostMigrationValidator
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.sync.OffenderMatchGroupFKValidator
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.sync.SyncPostMigrationValidator
import java.util.UUID
import javax.sql.DataSource

@Configuration
@EnableBatchProcessing
class OffenderMatchGroupBatchConfig(
  private val jobRepository: JobRepository,
  private val transactionManager: PlatformTransactionManager,
  @Qualifier("sourceDataSource") private val sourceDataSource: DataSource,
  @Qualifier("targetDataSource") private val targetDataSource: DataSource,
  private val batchProperties: BatchProperties,
) {

  private val log = LoggerFactory.getLogger(OffenderMatchGroupBatchConfig::class.java)

  @Autowired
  lateinit var jobLauncher: JobLauncher

  @Bean
  @StepScope
  fun offenderMatchGroupReader(
    @Value("#{jobParameters['minId']}") minId: Long?,
    @Value("#{jobParameters['maxId']}") maxId: Long?,
  ): JdbcCursorItemReader<OffenderMatchGroupQueryResult> = JdbcCursorItemReaderBuilder<OffenderMatchGroupQueryResult>()
    .name("offenderMatchGroupReader")
    .dataSource(sourceDataSource)
    .fetchSize(3000)
    .sql("$SOURCE_QUERY WHERE omg.id BETWEEN $minId AND $maxId order by omg.id")
    .rowMapper { rs, _ ->
      OffenderMatchGroupQueryResult(
        id = rs.getInt("id"),
        caseId = rs.getObject("case_id", Integer::class.java)?.toInt(),
        defendantId = rs.getObject("defendant_id", Integer::class.java)?.toInt(),
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
  fun offenderMatchGroupProcessor(): ItemProcessor<OffenderMatchGroupQueryResult, OffenderMatchGroup> = CompositeItemProcessorBuilder<OffenderMatchGroupQueryResult, OffenderMatchGroup>()
    .delegates(listOf(OffenderMatchGroupProcessor()))
    .build()

  @Bean
  fun offenderMatchGroupWriter(): JdbcBatchItemWriter<OffenderMatchGroup> = JdbcBatchItemWriterBuilder<OffenderMatchGroup>()
    .itemSqlParameterSourceProvider(BeanPropertyItemSqlParameterSourceProvider())
    .sql(
      """INSERT INTO hmpps_court_case_service.offender_match_group (id, legacy_id, legacy_defendant_id, legacy_prosecution_case_id, created_at, created_by, updated_at, updated_by, is_deleted, version)
        VALUES (:id, :legacyID, :legacyDefendantID, :legacyProsecutionCaseID, :createdAt, :createdBy, :updatedAt, :updatedBy, :isDeleted, :version)""",
    )
    .dataSource(targetDataSource)
    .build()

  @Bean
  fun offenderMatchGroupSkipListener() = object : SkipListener<OffenderMatchGroupQueryResult, OffenderMatchGroup> {
    override fun onSkipInRead(t: Throwable) {
      log.warn("Skipped during read: ${t.message}")
    }

    override fun onSkipInProcess(item: OffenderMatchGroupQueryResult, t: Throwable) {
      log.warn("Skipped during process: ${item.id}, reason: ${t.message}")
    }

    override fun onSkipInWrite(item: OffenderMatchGroup, t: Throwable) {
      log.warn("Skipped during write: ${item.id}, reason: ${t.message}")
    }
  }

  @Bean
  fun offenderMatchGroupStep(): Step = StepBuilder("offenderMatchGroupStep", jobRepository)
    .chunk<OffenderMatchGroupQueryResult, OffenderMatchGroup>(batchProperties.chunkSize, transactionManager)
    .reader(offenderMatchGroupReader(null, null))
    .processor(offenderMatchGroupProcessor())
    .writer(offenderMatchGroupWriter())
    .listener(offenderMatchGroupSkipListener())
    .faultTolerant()
    .retry(Throwable::class.java)
    .retryLimit(3)
    .build()

  @Bean
  fun offenderMatchGroupRowCountListener(): RowCountListener = RowCountListener(
    sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
    targetJdbcTemplate = JdbcTemplate(targetDataSource),
    sourceRowCountQuery = SOURCE_ROW_COUNT_QUERY,
    targetRowCountQuery = TARGET_ROW_COUNT_QUERY,
  )

  fun validationStep(): Step = StepBuilder("validationStep", jobRepository)
    .tasklet(validationTasklet(), transactionManager)
    .build()

  fun validationTasklet(): Tasklet {
    val strategy = OffenderMatchGroupValidator(
      sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
      targetJdbcTemplate = JdbcTemplate(targetDataSource),
    )
    return PostMigrationValidator(strategy, 100)
  }

  @Bean
  fun offenderMatchGroupJob(timerJobListener: TimerJobListener): Job = JobBuilder(
    "offenderMatchGroupJob",
    jobRepository,
  )
    .incrementer(RunIdIncrementer())
    .listener(timerJobListener)
    .listener(offenderMatchGroupRowCountListener())
    .start(offenderMatchGroupStep())
    .next(validationStep())
    .build()

  @Bean(name = ["offenderMatchGroupJobService"])
  fun offenderMatchGroupJobService(@Qualifier("offenderMatchGroupJob") offenderMatchGroupJob: Job): JobService = JobService(
    jobLauncher = jobLauncher,
    job = offenderMatchGroupJob,
    jdbcTemplate = JdbcTemplate(sourceDataSource),
    batchSize = 15,
    minQuery = MIN_QUERY,
    maxQuery = MAX_QUERY,
    jobName = "OffenderMatchGroup",
  )

  @Bean
  fun offenderMatchGroupJobScheduler(dataSource: DataSource, timerJobListener: TimerJobListener) = JobScheduler(
    jobService = offenderMatchGroupJobService(offenderMatchGroupJob(timerJobListener)),
    jobType = JobType.OFFENDER_MATCH_GROUP,
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

  @Bean(name = ["syncDefendantIdInOffenderMatchGroupJobService"])
  fun syncDefendantIdInOffenderMatchGroupJobService(@Qualifier("syncDefendantIdInOffenderMatchGroupJob") syncDefendantIdInOffenderMatchGroupJob: Job): JobService = JobService(
    jobLauncher = jobLauncher,
    job = syncDefendantIdInOffenderMatchGroupJob,
    jdbcTemplate = JdbcTemplate(targetDataSource),
    batchSize = 15,
    minQuery = SYNC_DEFENDANT_ID_MIN_QUERY,
    maxQuery = SYNC_DEFENDANT_ID_MAX_QUERY,
    jobName = "syncDefendantIdInOffenderMatchGroup",
  )

  fun offenderMatchGroupFKValidationStep(): Step = StepBuilder("offenderMatchGroupFKValidationStep", jobRepository)
    .tasklet(offenderMatchGroupFKValidationTasklet(), transactionManager)
    .build()

  fun offenderMatchGroupFKValidationTasklet(): Tasklet {
    val strategy = OffenderMatchGroupFKValidator(
      sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
      targetJdbcTemplate = JdbcTemplate(targetDataSource),
    )
    return SyncPostMigrationValidator(strategy, 100)
  }

  @Bean
  fun syncDefendantIdInOffenderMatchGroupJob(
    timerJobListener: TimerJobListener,
  ): Job = JobBuilder("syncDefendantIdInOffenderMatchGroupJob", jobRepository)
    .incrementer(RunIdIncrementer())
    .listener(timerJobListener)
    .start(syncDefendantIdInOffenderMatchGroupStep())
    .next(offenderMatchGroupFKValidationStep())
    .build()

  @Bean
  @StepScope
  fun syncDefendantIdInOffenderMatchGroupReader(
    @Value("#{jobParameters['minId']}") minId: Long?,
    @Value("#{jobParameters['maxId']}") maxId: Long?,
  ): JdbcCursorItemReader<Defendant> = JdbcCursorItemReaderBuilder<Defendant>()
    .name("syncDefendantIdInOffenderMatchGroupReader")
    .dataSource(targetDataSource)
    .fetchSize(3000)
    .sql("${SYNC_DEFENDANT_ID_QUERY} WHERE d.legacy_id BETWEEN $minId AND $maxId ORDER BY d.id ASC")
    .rowMapper { rs, _ ->
      Defendant(
        id = rs.getObject("id", UUID::class.java),
        legacyID = rs.getLong("legacy_id"),
        defendantID = null,
        isManualUpdate = null,
        crn = null,
        croNumber = null,
        tsvName = null,
        pncId = null,
        cprUUID = null,
        isOffenderConfirmed = null,
        person = null,
        offenderID = null,
        legacyOffenderID = null,
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
  fun syncDefendantIdInOffenderMatchGroupProcessor(): ItemProcessor<Defendant, OffenderMatchGroup> = CompositeItemProcessorBuilder<Defendant, OffenderMatchGroup>()
    .delegates(listOf(SyncDefendantIdInOffenderMatchGroupProcessor()))
    .build()

  @Bean
  fun syncDefendantIdInOffenderMatchGroupWriter(): JdbcBatchItemWriter<OffenderMatchGroup> = JdbcBatchItemWriterBuilder<OffenderMatchGroup>()
    .itemSqlParameterSourceProvider(BeanPropertyItemSqlParameterSourceProvider())
    .sql(
      """UPDATE hmpps_court_case_service.offender_match_group SET defendant_id = :defendantID WHERE legacy_defendant_id = :legacyDefendantID""",
    )
    .dataSource(targetDataSource)
    .assertUpdates(false)
    .build()

  @Bean
  fun syncDefendantIdInOffenderMatchGroupStep(): Step = StepBuilder("syncDefendantIdInOffenderMatchGroupStep", jobRepository)
    .chunk<Defendant, OffenderMatchGroup>(batchProperties.chunkSize, transactionManager)
    .reader(syncDefendantIdInOffenderMatchGroupReader(null, null))
    .processor(syncDefendantIdInOffenderMatchGroupProcessor())
    .writer(syncDefendantIdInOffenderMatchGroupWriter())
    .faultTolerant()
    .retry(Throwable::class.java)
    .retryLimit(3)
    .build()

  /**
   *
   *
   * Update ProsecutionCaseID FK job below.
   *
   *
   *
   */

  @Bean(name = ["syncProsecutionCaseIdInOffenderMatchGroupJobService"])
  fun syncProsecutionCaseIdInOffenderMatchGroupJobService(@Qualifier("syncProsecutionCaseIdInOffenderMatchGroupJob") syncProsecutionCaseIdInOffenderMatchGroupJob: Job): JobService = JobService(
    jobLauncher = jobLauncher,
    job = syncProsecutionCaseIdInOffenderMatchGroupJob,
    jdbcTemplate = JdbcTemplate(targetDataSource),
    batchSize = 15,
    minQuery = SYNC_PROSECUTION_CASE_ID_MIN_QUERY,
    maxQuery = SYNC_PROSECUTION_CASE_ID_MAX_QUERY,
    jobName = "syncProsecutionCaseIdInOffenderMatchGroup",
  )

  @Bean
  fun syncProsecutionCaseIdInOffenderMatchGroupJob(timerJobListener: TimerJobListener): Job = JobBuilder("syncProsecutionCaseIdInOffenderMatchGroupJob", jobRepository)
    .incrementer(RunIdIncrementer())
    .listener(timerJobListener)
    .start(syncProsecutionCaseIdInOffenderMatchGroupStep())
    .build()

  @Bean
  @StepScope
  fun syncProsecutionCaseIdInOffenderMatchGroupReader(
    @Value("#{jobParameters['minId']}") minId: Long?,
    @Value("#{jobParameters['maxId']}") maxId: Long?,
  ): JdbcCursorItemReader<ProsecutionCase> = JdbcCursorItemReaderBuilder<ProsecutionCase>()
    .name("syncProsecutionCaseIdInOffenderMatchGroupReader")
    .dataSource(targetDataSource)
    .fetchSize(3000)
    .sql("${SYNC_PROSECUTION_CASE_ID_QUERY} WHERE pc.legacy_id BETWEEN $minId AND $maxId ORDER BY pc.id ASC")
    .rowMapper { rs, _ ->
      ProsecutionCase(
        id = rs.getObject("id", UUID::class.java),
        legacyID = rs.getLong("legacy_id"),
        caseID = null,
        caseURN = null,
        cID = null,
        sourceType = null,
        caseMarkers = null,
        caseDocuments = null,
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
  fun syncProsecutionCaseIdInOffenderMatchGroupProcessor(): ItemProcessor<ProsecutionCase, OffenderMatchGroup> = CompositeItemProcessorBuilder<ProsecutionCase, OffenderMatchGroup>()
    .delegates(listOf(SyncProsecutionCaseIdInOffenderMatchGroupProcessor()))
    .build()

  @Bean
  fun syncProsecutionCaseIdInOffenderMatchGroupWriter(): JdbcBatchItemWriter<OffenderMatchGroup> = JdbcBatchItemWriterBuilder<OffenderMatchGroup>()
    .itemSqlParameterSourceProvider(BeanPropertyItemSqlParameterSourceProvider())
    .sql(
      """UPDATE hmpps_court_case_service.offender_match_group SET prosecution_case_id = :prosecutionCaseID WHERE legacy_prosecution_case_id = :legacyProsecutionCaseID""",
    )
    .dataSource(targetDataSource)
    .assertUpdates(false)
    .build()

  @Bean
  fun syncProsecutionCaseIdInOffenderMatchGroupStep(): Step = StepBuilder("syncProsecutionCaseIdInOffenderMatchGroupStep", jobRepository)
    .chunk<ProsecutionCase, OffenderMatchGroup>(batchProperties.chunkSize, transactionManager)
    .reader(syncProsecutionCaseIdInOffenderMatchGroupReader(null, null))
    .processor(syncProsecutionCaseIdInOffenderMatchGroupProcessor())
    .writer(syncProsecutionCaseIdInOffenderMatchGroupWriter())
    .faultTolerant()
    .retry(Throwable::class.java)
    .retryLimit(3)
    .build()
}
