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
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantOffenceConstants.MAX_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantOffenceConstants.MIN_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantOffenceConstants.SOURCE_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantOffenceConstants.SOURCE_ROW_COUNT_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantOffenceConstants.SYNC_DEFENDANT_ID_MAX_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantOffenceConstants.SYNC_DEFENDANT_ID_MIN_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantOffenceConstants.SYNC_DEFENDANT_ID_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantOffenceConstants.SYNC_DEFENDANT_ID_SOURCE_ROW_COUNT_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantOffenceConstants.SYNC_DEFENDANT_ID_TARGET_ROW_COUNT_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantOffenceConstants.SYNC_OFFENCE_ID_MAX_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantOffenceConstants.SYNC_OFFENCE_ID_MIN_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantOffenceConstants.SYNC_OFFENCE_ID_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantOffenceConstants.SYNC_OFFENCE_ID_SOURCE_ROW_COUNT_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantOffenceConstants.SYNC_OFFENCE_ID_TARGET_ROW_COUNT_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantOffenceConstants.TARGET_ROW_COUNT_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.JobType
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.DefendantOffenceQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Defendant
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.DefendantOffence
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Offence
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.RowCountListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.TimerJobListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.DefendantOffenceProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.SyncDefendantIdInDefendantOffenceProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.SyncOffenceIdInDefendantOffenceProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler.JobScheduler
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler.SchedulingConfigRepository
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service.JobService
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.DefendantOffenceValidator
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.PostMigrationValidator
import java.util.UUID
import javax.sql.DataSource

@Configuration
@EnableBatchProcessing
class DefendantOffenceBatchConfig(
  private val jobRepository: JobRepository,
  private val transactionManager: PlatformTransactionManager,
  @Qualifier("sourceDataSource") private val sourceDataSource: DataSource,
  @Qualifier("targetDataSource") private val targetDataSource: DataSource,
  private val batchProperties: BatchProperties,
) {

  private val log = LoggerFactory.getLogger(DefendantOffenceBatchConfig::class.java)

  @Autowired
  lateinit var jobLauncher: JobLauncher

  @Bean
  @StepScope
  fun defendantOffenceReader(
    @Value("#{jobParameters['minId']}") minId: Long?,
    @Value("#{jobParameters['maxId']}") maxId: Long?,
  ): JdbcCursorItemReader<DefendantOffenceQueryResult> = JdbcCursorItemReaderBuilder<DefendantOffenceQueryResult>()
    .name("defendantOffenceReader")
    .dataSource(sourceDataSource)
    .fetchSize(3000)
    .sql("${SOURCE_QUERY} WHERE hd.id BETWEEN $minId AND $maxId ORDER BY hd.id ASC")
    .rowMapper { rs, _ ->
      DefendantOffenceQueryResult(
        id = rs.getInt("id"),
        legacyOffenceId = rs.getInt("legacy_offence_id"),
        legacyDefendantId = rs.getInt("legacy_defendant_id"),
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
  fun defendantOffenceProcessor(): ItemProcessor<DefendantOffenceQueryResult, DefendantOffence> = CompositeItemProcessorBuilder<DefendantOffenceQueryResult, DefendantOffence>()
    .delegates(listOf(DefendantOffenceProcessor()))
    .build()

  @Bean
  fun defendantOffenceWriter(): JdbcBatchItemWriter<DefendantOffence> = JdbcBatchItemWriterBuilder<DefendantOffence>()
    .itemSqlParameterSourceProvider(BeanPropertyItemSqlParameterSourceProvider())
    .sql(
      """INSERT INTO hmpps_court_case_service.defendant_offence (id, offence_id, defendant_id, legacy_offence_id, legacy_defendant_id, created_at, created_by, updated_at, updated_by, is_deleted, version)
        VALUES (:id, :offenceID, :defendantID, :legacyOffenceID, :legacyDefendantID, :createdAt, :createdBy, :updatedAt, :updatedBy, :isDeleted, :version)""",
    )
    .dataSource(targetDataSource)
    .build()

  @Bean
  fun defendantOffenceSkipListener() = object : SkipListener<DefendantOffenceQueryResult, DefendantOffence> {
    override fun onSkipInRead(t: Throwable) {
      log.warn("Skipped during read: ${t.message}")
    }

    override fun onSkipInProcess(item: DefendantOffenceQueryResult, t: Throwable) {
      log.warn("Skipped during process: ${item.id}, reason: ${t.message}")
    }

    override fun onSkipInWrite(item: DefendantOffence, t: Throwable) {
      log.warn("Skipped during write: ${item.id}, reason: ${t.message}")
    }
  }

  @Bean
  fun defendantOffenceStep(): Step = StepBuilder("defendantOffenceStep", jobRepository)
    .chunk<DefendantOffenceQueryResult, DefendantOffence>(batchProperties.chunkSize, transactionManager)
    .reader(defendantOffenceReader(null, null))
    .processor(defendantOffenceProcessor())
    .writer(defendantOffenceWriter())
    .listener(defendantOffenceSkipListener())
    .faultTolerant()
    .retry(Throwable::class.java)
    .retryLimit(3)
    .build()

  @Bean
  fun defendantOffenceRowCountListener(): RowCountListener = RowCountListener(
    sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
    targetJdbcTemplate = JdbcTemplate(targetDataSource),
    sourceRowCountQuery = SOURCE_ROW_COUNT_QUERY,
    targetRowCountQuery = TARGET_ROW_COUNT_QUERY,
  )

  fun validationStep(): Step = StepBuilder("validationStep", jobRepository)
    .tasklet(validationTasklet(), transactionManager)
    .build()

  fun validationTasklet(): Tasklet {
    val strategy = DefendantOffenceValidator(
      sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
      targetJdbcTemplate = JdbcTemplate(targetDataSource),
    )
    return PostMigrationValidator(strategy, 100)
  }

  @Bean
  fun defendantOffenceJob(timerJobListener: TimerJobListener): Job = JobBuilder("defendantOffenceJob", jobRepository)
    .incrementer(RunIdIncrementer())
    .listener(timerJobListener)
    .listener(defendantOffenceRowCountListener())
    .start(defendantOffenceStep())
    .next(validationStep())
    .build()

  @Bean(name = ["defendantOffenceJobService"])
  fun defendantOffenceJobService(@Qualifier("defendantOffenceJob") defendantOffenceJob: Job): JobService = JobService(
    jobLauncher = jobLauncher,
    job = defendantOffenceJob,
    jdbcTemplate = JdbcTemplate(sourceDataSource),
    batchSize = 15,
    minQuery = MIN_QUERY,
    maxQuery = MAX_QUERY,
    jobName = "DefendantOffence",
  )

  @Bean
  fun defendantOffenceJobScheduler(dataSource: DataSource, timerJobListener: TimerJobListener) = JobScheduler(
    jobService = defendantOffenceJobService(defendantOffenceJob(timerJobListener)),
    jobType = JobType.DEFENDANT_OFFENCE,
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

  @Bean
  fun syncDefendantIdInDefendantOffenceRowCountListener(): RowCountListener = RowCountListener(
    sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
    targetJdbcTemplate = JdbcTemplate(targetDataSource),
    sourceRowCountQuery = SYNC_DEFENDANT_ID_SOURCE_ROW_COUNT_QUERY,
    targetRowCountQuery = SYNC_DEFENDANT_ID_TARGET_ROW_COUNT_QUERY,
  )

  @Bean(name = ["syncDefendantIdInDefendantOffenceJobService"])
  fun syncDefendantIdInDefendantOffenceJobService(@Qualifier("syncDefendantIdInDefendantOffenceJob") syncDefendantIdInDefendantOffenceJob: Job): JobService = JobService(
    jobLauncher = jobLauncher,
    job = syncDefendantIdInDefendantOffenceJob,
    jdbcTemplate = JdbcTemplate(targetDataSource),
    batchSize = 15,
    minQuery = SYNC_DEFENDANT_ID_MIN_QUERY,
    maxQuery = SYNC_DEFENDANT_ID_MAX_QUERY,
    jobName = "syncDefendantIdInDefendantOffence",
  )

  @Bean
  fun syncDefendantIdInDefendantOffenceJob(timerJobListener: TimerJobListener): Job = JobBuilder("syncDefendantIdInDefendantOffenceJob", jobRepository)
    .incrementer(RunIdIncrementer())
    .listener(timerJobListener)
    .listener(syncDefendantIdInDefendantOffenceRowCountListener())
    .start(syncDefendantIdInDefendantOffenceStep())
    .build()

  @Bean
  @StepScope
  fun syncDefendantIdInDefendantOffenceReader(
    @Value("#{jobParameters['minId']}") minId: Long?,
    @Value("#{jobParameters['maxId']}") maxId: Long?,
  ): JdbcCursorItemReader<Defendant> = JdbcCursorItemReaderBuilder<Defendant>()
    .name("syncDefendantIdInDefendantOffenceReader")
    .dataSource(targetDataSource)
    .fetchSize(3000)
    .sql("${SYNC_DEFENDANT_ID_QUERY} WHERE d.legacy_id BETWEEN $minId AND $maxId ORDER BY d.id ASC")
    .rowMapper { rs, _ ->
      Defendant(
        id = rs.getObject("id", UUID::class.java),
        defendantID = rs.getObject("defendant_id", UUID::class.java),
        legacyID = rs.getLong("legacy_id"),
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
  fun syncDefendantIdInDefendantOffenceProcessor(): ItemProcessor<Defendant, DefendantOffence> = CompositeItemProcessorBuilder<Defendant, DefendantOffence>()
    .delegates(listOf(SyncDefendantIdInDefendantOffenceProcessor()))
    .build()

  @Bean
  fun syncDefendantIdInDefendantOffenceWriter(): JdbcBatchItemWriter<DefendantOffence> = JdbcBatchItemWriterBuilder<DefendantOffence>()
    .itemSqlParameterSourceProvider(BeanPropertyItemSqlParameterSourceProvider())
    .sql(
      """UPDATE hmpps_court_case_service.defendant_offence SET defendant_id = :defendantID WHERE legacy_defendant_id = :legacyDefendantID""",
    )
    .dataSource(targetDataSource)
    .assertUpdates(false)
    .build()

  @Bean
  fun syncDefendantIdInDefendantOffenceStep(): Step = StepBuilder("syncDefendantIdInDefendantOffenceStep", jobRepository)
    .chunk<Defendant, DefendantOffence>(batchProperties.chunkSize, transactionManager)
    .reader(syncDefendantIdInDefendantOffenceReader(null, null))
    .processor(syncDefendantIdInDefendantOffenceProcessor())
    .writer(syncDefendantIdInDefendantOffenceWriter())
    .faultTolerant()
    .retry(Throwable::class.java)
    .retryLimit(3)
    .build()

  /**
   *
   *
   * Update OffenceID FK job below.
   *
   *
   *
   */

  @Bean
  fun syncOffenceIdInDefendantOffenceRowCountListener(): RowCountListener = RowCountListener(
    sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
    targetJdbcTemplate = JdbcTemplate(targetDataSource),
    sourceRowCountQuery = SYNC_OFFENCE_ID_SOURCE_ROW_COUNT_QUERY,
    targetRowCountQuery = SYNC_OFFENCE_ID_TARGET_ROW_COUNT_QUERY,
  )

  @Bean(name = ["syncOffenceIdInDefendantOffenceJobService"])
  fun syncOffenceIdInDefendantOffenceJobService(@Qualifier("syncOffenceIdInDefendantOffenceJob") syncOffenceIdInDefendantOffenceJob: Job): JobService = JobService(
    jobLauncher = jobLauncher,
    job = syncOffenceIdInDefendantOffenceJob,
    jdbcTemplate = JdbcTemplate(targetDataSource),
    batchSize = 15,
    minQuery = SYNC_OFFENCE_ID_MIN_QUERY,
    maxQuery = SYNC_OFFENCE_ID_MAX_QUERY,
    jobName = "syncOffenceIdInDefendantOffence",
  )

  @Bean
  fun syncOffenceIdInDefendantOffenceJob(timerJobListener: TimerJobListener): Job = JobBuilder("syncOffenceIdInDefendantOffenceJob", jobRepository)
    .incrementer(RunIdIncrementer())
    .listener(timerJobListener)
    .listener(syncOffenceIdInDefendantOffenceRowCountListener())
    .start(syncOffenceIdInDefendantOffenceStep())
    .build()

  @Bean
  @StepScope
  fun syncOffenceIdInDefendantOffenceReader(
    @Value("#{jobParameters['minId']}") minId: Long?,
    @Value("#{jobParameters['maxId']}") maxId: Long?,
  ): JdbcCursorItemReader<Offence> = JdbcCursorItemReaderBuilder<Offence>()
    .name("syncOffenceIdInDefendantOffenceReader")
    .dataSource(targetDataSource)
    .fetchSize(3000)
    .sql("${SYNC_OFFENCE_ID_QUERY} WHERE o.legacy_id BETWEEN $minId AND $maxId ORDER BY o.id ASC")
    .rowMapper { rs, _ ->
      Offence(
        id = rs.getObject("id", UUID::class.java),
        legacyID = rs.getLong("legacy_id"),
        createdAt = null,
        createdBy = null,
        updatedAt = null,
        updatedBy = null,
        isDeleted = null,
        version = null,
        code = null,
        title = null,
        legislation = null,
        listingNumber = null,
        sequence = null,
        shortTermCustodyPredictorScore = null,
        wording = null,
        judicialResults = null,
        plea = null,
        verdict = null,
      )
    }
    .build()

  @Bean
  fun syncOffenceIdInDefendantOffenceProcessor(): ItemProcessor<Offence, DefendantOffence> = CompositeItemProcessorBuilder<Offence, DefendantOffence>()
    .delegates(listOf(SyncOffenceIdInDefendantOffenceProcessor()))
    .build()

  @Bean
  fun syncOffenceIdInDefendantOffenceWriter(): JdbcBatchItemWriter<DefendantOffence> = JdbcBatchItemWriterBuilder<DefendantOffence>()
    .itemSqlParameterSourceProvider(BeanPropertyItemSqlParameterSourceProvider())
    .sql(
      """UPDATE hmpps_court_case_service.defendant_offence SET offence_id = :offenceID WHERE legacy_offence_id = :legacyOffenceID""",
    )
    .dataSource(targetDataSource)
    .assertUpdates(false)
    .build()

  @Bean
  fun syncOffenceIdInDefendantOffenceStep(): Step = StepBuilder("syncOffenceIdInDefendantOffenceStep", jobRepository)
    .chunk<Offence, DefendantOffence>(batchProperties.chunkSize, transactionManager)
    .reader(syncOffenceIdInDefendantOffenceReader(null, null))
    .processor(syncOffenceIdInDefendantOffenceProcessor())
    .writer(syncOffenceIdInDefendantOffenceWriter())
    .faultTolerant()
    .retry(Throwable::class.java)
    .retryLimit(3)
    .build()
}
