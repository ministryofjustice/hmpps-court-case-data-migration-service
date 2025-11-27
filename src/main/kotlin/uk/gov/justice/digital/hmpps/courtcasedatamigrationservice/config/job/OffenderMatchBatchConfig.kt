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
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchConstants.MAX_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchConstants.MIN_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchConstants.SOURCE_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchConstants.SOURCE_ROW_COUNT_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchConstants.SYNC_OFFENDER_ID_MAX_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchConstants.SYNC_OFFENDER_ID_MIN_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchConstants.SYNC_OFFENDER_ID_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchConstants.SYNC_OFFENDER_MATCH_GROUP_ID_MAX_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchConstants.SYNC_OFFENDER_MATCH_GROUP_ID_MIN_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchConstants.SYNC_OFFENDER_MATCH_GROUP_ID_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.OffenderMatchConstants.TARGET_ROW_COUNT_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.JobType
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.OffenderMatchQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Offender
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.OffenderMatch
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.OffenderMatchGroup
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.RowCountListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.TimerJobListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.OffenderMatchProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.sync.SyncOffenderIdInOffenderMatchProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.sync.SyncOffenderMatchGroupIdInOffenderMatchProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler.JobScheduler
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler.SchedulingConfigRepository
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service.JobService
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.OffenderMatchValidator
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.PostMigrationValidator
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.sync.OffenderMatchFKValidator
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.sync.SyncPostMigrationValidator
import java.util.UUID
import javax.sql.DataSource

@Configuration
@EnableBatchProcessing
class OffenderMatchBatchConfig(
  private val jobRepository: JobRepository,
  private val transactionManager: PlatformTransactionManager,
  @Qualifier("sourceDataSource") private val sourceDataSource: DataSource,
  @Qualifier("targetDataSource") private val targetDataSource: DataSource,
  private val batchProperties: BatchProperties,
) {

  private val log = LoggerFactory.getLogger(OffenderMatchBatchConfig::class.java)

  @Autowired
  lateinit var jobLauncher: JobLauncher

  @Bean
  @StepScope
  fun offenderMatchReader(
    @Value("#{jobParameters['minId']}") minId: Long?,
    @Value("#{jobParameters['maxId']}") maxId: Long?,
  ): JdbcCursorItemReader<OffenderMatchQueryResult> = JdbcCursorItemReaderBuilder<OffenderMatchQueryResult>()
    .name("offenderMatchReader")
    .dataSource(sourceDataSource)
    .fetchSize(3000)
    .sql("${SOURCE_QUERY} WHERE om.id BETWEEN $minId AND $maxId order by om.id asc")
    .rowMapper { rs, _ ->
      OffenderMatchQueryResult(
        id = rs.getInt("id"),
        fkOffenderId = rs.getLong("fk_offender_id"),
        groupId = rs.getLong("group_id"),
        matchType = rs.getString("match_type"),
        aliases = rs.getString("aliases"),
        rejected = rs.getBoolean("rejected"),
        matchProbability = rs.getBigDecimal("match_probability"),
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
  fun offenderMatchProcessor(): ItemProcessor<OffenderMatchQueryResult, OffenderMatch> = CompositeItemProcessorBuilder<OffenderMatchQueryResult, OffenderMatch>()
    .delegates(listOf(OffenderMatchProcessor()))
    .build()

  @Bean
  fun offenderMatchWriter(): JdbcBatchItemWriter<OffenderMatch> = JdbcBatchItemWriterBuilder<OffenderMatch>()
    .itemSqlParameterSourceProvider(BeanPropertyItemSqlParameterSourceProvider())
    .sql(
      """INSERT INTO hmpps_court_case_service.offender_match (id, legacy_id, offender_id, offender_match_group_id, legacy_offender_id, legacy_offender_match_group_id, match_type, is_rejected, aliases, match_probability, created_at, created_by, updated_at, updated_by, is_deleted, version)
        VALUES (:id, :legacyID, :offenderID, :offenderMatchGroupID, :legacyOffenderID, :legacyOffenderMatchGroupID, :matchType, :isRejected, CAST(:aliases AS jsonb), :matchProbability, :createdAt, :createdBy, :updatedAt, :updatedBy, :isDeleted, :version)""",
    )
    .dataSource(targetDataSource)
    .build()

  @Bean
  fun offenderMatchSkipListener() = object : SkipListener<OffenderMatchQueryResult, OffenderMatch> {
    override fun onSkipInRead(t: Throwable) {
      log.warn("Skipped during read: ${t.message}")
    }

    override fun onSkipInProcess(item: OffenderMatchQueryResult, t: Throwable) {
      log.warn("Skipped during process: ${item.id}, reason: ${t.message}")
    }

    override fun onSkipInWrite(item: OffenderMatch, t: Throwable) {
      log.warn("Skipped during write: ${item.id}, reason: ${t.message}")
    }
  }

  @Bean
  fun offenderMatchStep(): Step = StepBuilder("offenderMatchStep", jobRepository)
    .chunk<OffenderMatchQueryResult, OffenderMatch>(batchProperties.chunkSize, transactionManager)
    .reader(offenderMatchReader(null, null))
    .processor(offenderMatchProcessor())
    .writer(offenderMatchWriter())
    .listener(offenderMatchSkipListener())
    .faultTolerant()
    .retry(Throwable::class.java)
    .retryLimit(3)
    .build()

  @Bean
  fun offenderMatchRowCountListener(): RowCountListener = RowCountListener(
    sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
    targetJdbcTemplate = JdbcTemplate(targetDataSource),
    sourceRowCountQuery = SOURCE_ROW_COUNT_QUERY,
    targetRowCountQuery = TARGET_ROW_COUNT_QUERY,
  )

  fun validationStep(): Step = StepBuilder("validationStep", jobRepository)
    .tasklet(validationTasklet(), transactionManager)
    .build()

  fun validationTasklet(): Tasklet {
    val strategy = OffenderMatchValidator(
      sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
      targetJdbcTemplate = JdbcTemplate(targetDataSource),
    )
    return PostMigrationValidator(strategy, 100)
  }

  @Bean
  fun offenderMatchJob(timerJobListener: TimerJobListener): Job = JobBuilder("offenderMatchJob", jobRepository)
    .incrementer(RunIdIncrementer())
    .listener(timerJobListener)
    .listener(offenderMatchRowCountListener())
    .start(offenderMatchStep())
    .next(validationStep())
    .build()

  @Bean(name = ["offenderMatchJobService"])
  fun offenderMatchJobService(@Qualifier("offenderMatchJob") offenderMatchJob: Job): JobService = JobService(
    jobLauncher = jobLauncher,
    job = offenderMatchJob,
    jdbcTemplate = JdbcTemplate(sourceDataSource),
    batchSize = 15,
    minQuery = MIN_QUERY,
    maxQuery = MAX_QUERY,
    jobName = "OffenderMatch",
  )

  @Bean
  fun offenderMatchJobScheduler(dataSource: DataSource, timerJobListener: TimerJobListener) = JobScheduler(
    jobService = offenderMatchJobService(offenderMatchJob(timerJobListener)),
    jobType = JobType.OFFENDER_MATCH,
    schedulingConfigRepository = SchedulingConfigRepository(JdbcTemplate(dataSource)),
  )

  /**
   *
   *
   * Update OffenderID FK job below.
   *
   *
   *
   */

  @Bean(name = ["syncOffenderIdInOffenderMatchJobService"])
  fun syncOffenderIdInOffenderMatchJobService(@Qualifier("syncOffenderIdInOffenderMatchJob") syncOffenderIdInOffenderMatchJob: Job): JobService = JobService(
    jobLauncher = jobLauncher,
    job = syncOffenderIdInOffenderMatchJob,
    jdbcTemplate = JdbcTemplate(targetDataSource),
    batchSize = 15,
    minQuery = SYNC_OFFENDER_ID_MIN_QUERY,
    maxQuery = SYNC_OFFENDER_ID_MAX_QUERY,
    jobName = "syncOffenderIdInOffenderMatch",
  )

  fun offenderMatchFKValidationStep(): Step = StepBuilder("offenderMatchFKValidationStep", jobRepository)
    .tasklet(offenderMatchFKValidationTasklet(), transactionManager)
    .build()

  fun offenderMatchFKValidationTasklet(): Tasklet {
    val strategy = OffenderMatchFKValidator(
      sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
      targetJdbcTemplate = JdbcTemplate(targetDataSource),
    )
    return SyncPostMigrationValidator(strategy, 100)
  }

  @Bean
  fun syncOffenderIdInOffenderMatchJob(timerJobListener: TimerJobListener): Job = JobBuilder("syncOffenderIdInOffenderMatchJob", jobRepository)
    .incrementer(RunIdIncrementer())
    .listener(timerJobListener)
    .start(syncOffenderIdInOffenderMatchStep())
    .next(offenderMatchFKValidationStep())
    .build()

  @Bean
  @StepScope
  fun syncOffenderIdInOffenderMatchReader(
    @Value("#{jobParameters['minId']}") minId: Long?,
    @Value("#{jobParameters['maxId']}") maxId: Long?,
  ): JdbcCursorItemReader<Offender> = JdbcCursorItemReaderBuilder<Offender>()
    .name("syncOffenderIdInOffenderMatchReader")
    .dataSource(targetDataSource)
    .fetchSize(3000)
    .sql("${SYNC_OFFENDER_ID_QUERY} WHERE o.legacy_id BETWEEN $minId AND $maxId ORDER BY o.id ASC")
    .rowMapper { rs, _ ->
      Offender(
        id = rs.getObject("id", UUID::class.java),
        legacyID = rs.getLong("legacy_id"),
        suspendedSentenceOrder = null,
        breach = null,
        awaitingPSR = null,
        probationStatus = null,
        preSentenceActivity = null,
        previouslyKnownTerminationDate = null,
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
  fun syncOffenderIdInOffenderMatchProcessor(): ItemProcessor<Offender, OffenderMatch> = CompositeItemProcessorBuilder<Offender, OffenderMatch>()
    .delegates(listOf(SyncOffenderIdInOffenderMatchProcessor()))
    .build()

  @Bean
  fun syncOffenderIdInOffenderMatchWriter(): JdbcBatchItemWriter<OffenderMatch> = JdbcBatchItemWriterBuilder<OffenderMatch>()
    .itemSqlParameterSourceProvider(BeanPropertyItemSqlParameterSourceProvider())
    .sql(
      """UPDATE hmpps_court_case_service.offender_match SET offender_id = :offenderID WHERE legacy_offender_id = :legacyOffenderID""",
    )
    .dataSource(targetDataSource)
    .assertUpdates(false)
    .build()

  @Bean
  fun syncOffenderIdInOffenderMatchStep(): Step = StepBuilder("syncOffenderIdInOffenderMatchStep", jobRepository)
    .chunk<Offender, OffenderMatch>(batchProperties.chunkSize, transactionManager)
    .reader(syncOffenderIdInOffenderMatchReader(null, null))
    .processor(syncOffenderIdInOffenderMatchProcessor())
    .writer(syncOffenderIdInOffenderMatchWriter())
    .faultTolerant()
    .retry(Throwable::class.java)
    .retryLimit(3)
    .build()

  /**
   *
   *
   * Update OffenderMatchGroupID FK job below.
   *
   *
   *
   */

  @Bean(name = ["syncOffenderMatchGroupIdInOffenderMatchJobService"])
  fun syncOffenderMatchGroupIdInOffenderMatchJobService(@Qualifier("syncOffenderMatchGroupIdInOffenderMatchJob") syncOffenderMatchGroupIdInOffenderMatchJob: Job): JobService = JobService(
    jobLauncher = jobLauncher,
    job = syncOffenderMatchGroupIdInOffenderMatchJob,
    jdbcTemplate = JdbcTemplate(targetDataSource),
    batchSize = 15,
    minQuery = SYNC_OFFENDER_MATCH_GROUP_ID_MIN_QUERY,
    maxQuery = SYNC_OFFENDER_MATCH_GROUP_ID_MAX_QUERY,
    jobName = "syncOffenderMatchGroupIdInOffenderMatch",
  )

  @Bean
  fun syncOffenderMatchGroupIdInOffenderMatchJob(timerJobListener: TimerJobListener): Job = JobBuilder("syncOffenderMatchGroupIdInOffenderMatchJob", jobRepository)
    .incrementer(RunIdIncrementer())
    .listener(timerJobListener)
    .start(syncOffenderMatchGroupIdInOffenderMatchStep())
    .build()

  @Bean
  @StepScope
  fun syncOffenderMatchGroupIdInOffenderMatchReader(
    @Value("#{jobParameters['minId']}") minId: Long?,
    @Value("#{jobParameters['maxId']}") maxId: Long?,
  ): JdbcCursorItemReader<OffenderMatchGroup> = JdbcCursorItemReaderBuilder<OffenderMatchGroup>()
    .name("syncOffenderMatchGroupIdInOffenderMatchReader")
    .dataSource(targetDataSource)
    .fetchSize(3000)
    .sql("${SYNC_OFFENDER_MATCH_GROUP_ID_QUERY} WHERE omg.legacy_id BETWEEN $minId AND $maxId ORDER BY omg.id ASC")
    .rowMapper { rs, _ ->
      OffenderMatchGroup(
        id = rs.getObject("id", UUID::class.java),
        legacyID = rs.getLong("legacy_id"),
        prosecutionCaseID = null,
        legacyProsecutionCaseID = null,
        defendantID = null,
        legacyDefendantID = null,
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
  fun syncOffenderMatchGroupIdInOffenderMatchProcessor(): ItemProcessor<OffenderMatchGroup, OffenderMatch> = CompositeItemProcessorBuilder<OffenderMatchGroup, OffenderMatch>()
    .delegates(listOf(SyncOffenderMatchGroupIdInOffenderMatchProcessor()))
    .build()

  @Bean
  fun syncOffenderMatchGroupIdInOffenderMatchWriter(): JdbcBatchItemWriter<OffenderMatch> = JdbcBatchItemWriterBuilder<OffenderMatch>()
    .itemSqlParameterSourceProvider(BeanPropertyItemSqlParameterSourceProvider())
    .sql(
      """UPDATE hmpps_court_case_service.offender_match SET offender_match_group_id = :offenderMatchGroupID WHERE legacy_offender_match_group_id = :legacyOffenderMatchGroupID""",
    )
    .dataSource(targetDataSource)
    .assertUpdates(false)
    .build()

  @Bean
  fun syncOffenderMatchGroupIdInOffenderMatchStep(): Step = StepBuilder("syncOffenderMatchGroupIdInOffenderMatchStep", jobRepository)
    .chunk<OffenderMatchGroup, OffenderMatch>(batchProperties.chunkSize, transactionManager)
    .reader(syncOffenderMatchGroupIdInOffenderMatchReader(null, null))
    .processor(syncOffenderMatchGroupIdInOffenderMatchProcessor())
    .writer(syncOffenderMatchGroupIdInOffenderMatchWriter())
    .faultTolerant()
    .retry(Throwable::class.java)
    .retryLimit(3)
    .build()
}
