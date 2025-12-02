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
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantConstants.MAX_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantConstants.MIN_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantConstants.SOURCE_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantConstants.SOURCE_ROW_COUNT_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantConstants.SYNC_OFFENDER_ID_MAX_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantConstants.SYNC_OFFENDER_ID_MIN_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantConstants.SYNC_OFFENDER_ID_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.constant.DefendantConstants.TARGET_ROW_COUNT_QUERY
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.JobType
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.SyncJobType
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.DefendantQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Defendant
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Offender
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.RowCountListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.TimerJobListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.DefendantProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.sync.SyncOffenderIdInDefendantProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler.JobScheduler
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler.SchedulingConfigRepository
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler.SyncJobScheduler
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service.JobService
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.DefendantValidator
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.PostMigrationValidator
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.sync.DefendantFKValidator
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.sync.SyncPostMigrationValidator
import java.time.LocalDate
import java.util.UUID
import javax.sql.DataSource

@Configuration
@EnableBatchProcessing
class DefendantBatchConfig(
  private val jobRepository: JobRepository,
  private val transactionManager: PlatformTransactionManager,
  @Qualifier("sourceDataSource") private val sourceDataSource: DataSource,
  @Qualifier("targetDataSource") private val targetDataSource: DataSource,
  private val batchProperties: BatchProperties,
) {

  private val log = LoggerFactory.getLogger(DefendantBatchConfig::class.java)

  @Autowired
  lateinit var jobLauncher: JobLauncher

  @Bean
  @StepScope
  fun defendantReader(
    @Value("#{jobParameters['minId']}") minId: Long?,
    @Value("#{jobParameters['maxId']}") maxId: Long?,
  ): JdbcCursorItemReader<DefendantQueryResult> = JdbcCursorItemReaderBuilder<DefendantQueryResult>()
    .name("defendantReader")
    .dataSource(sourceDataSource)
    .fetchSize(3000)
    .sql("${SOURCE_QUERY} WHERE d.id BETWEEN $minId AND $maxId")
    .rowMapper { rs, _ ->
      DefendantQueryResult(
        id = rs.getInt("id"),
        defendantID = rs.getObject("defendant_id", UUID::class.java),
        isManualUpdate = rs.getBoolean("manual_update"),
        crn = rs.getString("crn"),
        cro = rs.getString("cro"),
        name = rs.getString("name"),
        dateOfBirth = rs.getObject("date_of_birth", LocalDate::class.java),
        offenderConfirmed = rs.getBoolean("offender_confirmed"),
        nationality1 = rs.getString("nationality_1"),
        nationality2 = rs.getString("nationality_2"),
        sex = rs.getString("sex"),
        phoneNumber = rs.getString("phone_number"),
        address = rs.getString("address"),
        tsvName = rs.getString("tsv_name"),
        pnc = rs.getString("pnc"),
        cprUUID = rs.getString("cpr_uuid"),
        fkOffenderID = rs.getObject("fk_offender_id") as Long?,
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
  fun defendantProcessor(): ItemProcessor<DefendantQueryResult, Defendant> = CompositeItemProcessorBuilder<DefendantQueryResult, Defendant>()
    .delegates(listOf(DefendantProcessor()))
    .build()

  @Bean
  fun defendantWriter(): JdbcBatchItemWriter<Defendant> = JdbcBatchItemWriterBuilder<Defendant>()
    .itemSqlParameterSourceProvider(BeanPropertyItemSqlParameterSourceProvider())
    .sql(
      """INSERT INTO hmpps_court_case_service.defendant (id, defendant_id, legacy_id, is_manual_update, crn, cro_number, tsv_name, pnc_id, cpr_uuid, is_offender_confirmed, person, legacy_offender_id, offender_id, created_at, created_by, updated_at, updated_by, is_deleted, version)
        VALUES (:id, :defendantID, :legacyID, :isManualUpdate, :crn, :croNumber, cast(:tsvName as tsvector), :pncId, :cprUUID, :isOffenderConfirmed, CAST(:person AS jsonb), :legacyOffenderID, :offenderID, :createdAt, :createdBy, :updatedAt, :updatedBy, :isDeleted, :version)""",
    )
    .dataSource(targetDataSource)
    .build()

  @Bean
  fun defendantSkipListener() = object : SkipListener<DefendantQueryResult, Defendant> {
    override fun onSkipInRead(t: Throwable) {
      log.warn("Skipped during read: ${t.message}")
    }

    override fun onSkipInProcess(item: DefendantQueryResult, t: Throwable) {
      log.warn("Skipped during process: ${item.id}, reason: ${t.message}")
    }

    override fun onSkipInWrite(item: Defendant, t: Throwable) {
      log.warn("Skipped during write: ${item.id}, reason: ${t.message}")
    }
  }

  @Bean
  fun defendantStep(): Step = StepBuilder("defendantStep", jobRepository)
    .chunk<DefendantQueryResult, Defendant>(batchProperties.chunkSize, transactionManager)
    .reader(defendantReader(null, null))
    .processor(defendantProcessor())
    .writer(defendantWriter())
    .listener(defendantSkipListener())
    .faultTolerant()
    .retry(Throwable::class.java)
    .retryLimit(3)
    .build()

  @Bean
  fun defendantRowCountListener(): RowCountListener = RowCountListener(
    sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
    targetJdbcTemplate = JdbcTemplate(targetDataSource),
    sourceRowCountQuery = SOURCE_ROW_COUNT_QUERY,
    targetRowCountQuery = TARGET_ROW_COUNT_QUERY,
  )

  fun validationStep(): Step = StepBuilder("validationStep", jobRepository)
    .tasklet(validationTasklet(), transactionManager)
    .build()

  fun validationTasklet(): Tasklet {
    val strategy = DefendantValidator(
      sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
      targetJdbcTemplate = JdbcTemplate(targetDataSource),
    )
    return PostMigrationValidator(strategy, 100)
  }

  @Bean
  fun defendantJob(timerJobListener: TimerJobListener): Job = JobBuilder("defendantJob", jobRepository)
    .incrementer(RunIdIncrementer())
    .listener(timerJobListener)
    .listener(defendantRowCountListener())
    .start(defendantStep())
    .next(validationStep())
    .build()

  @Bean(name = ["defendantJobService"])
  fun defendantJobService(@Qualifier("defendantJob") defendantJob: Job): JobService = JobService(
    jobLauncher = jobLauncher,
    job = defendantJob,
    jdbcTemplate = JdbcTemplate(sourceDataSource),
    batchSize = 15,
    minQuery = MIN_QUERY,
    maxQuery = MAX_QUERY,
    jobName = "Defendant",
  )

  @Bean
  fun defendantJobScheduler(dataSource: DataSource, timerJobListener: TimerJobListener) = JobScheduler(
    jobService = defendantJobService(defendantJob(timerJobListener)),
    jobType = JobType.DEFENDANT,
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

  @Bean(name = ["syncOffenderIdInDefendantJobService"])
  fun syncOffenderIdInDefendantJobService(@Qualifier("syncOffenderIdInDefendantJob") syncOffenderIdInDefendantJob: Job): JobService = JobService(
    jobLauncher = jobLauncher,
    job = syncOffenderIdInDefendantJob,
    jdbcTemplate = JdbcTemplate(targetDataSource),
    batchSize = 15,
    minQuery = SYNC_OFFENDER_ID_MIN_QUERY,
    maxQuery = SYNC_OFFENDER_ID_MAX_QUERY,
    jobName = "syncOffenderIdInDefendant",
  )

  fun defendantFKValidationStep(): Step = StepBuilder("defendantFKValidationStep", jobRepository)
    .tasklet(defendantFKValidationTasklet(), transactionManager)
    .build()

  fun defendantFKValidationTasklet(): Tasklet {
    val strategy = DefendantFKValidator(
      sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
      targetJdbcTemplate = JdbcTemplate(targetDataSource),
    )
    return SyncPostMigrationValidator(strategy, 100)
  }

  @Bean
  fun syncOffenderIdInDefendantJob(timerJobListener: TimerJobListener): Job = JobBuilder("syncOffenderIdInDefendantJob", jobRepository)
    .incrementer(RunIdIncrementer())
    .listener(timerJobListener)
    .start(syncOffenderIdInDefendantStep())
    .next(defendantFKValidationStep())
    .build()

  @Bean
  @StepScope
  fun syncOffenderIdInDefendantReader(
    @Value("#{jobParameters['minId']}") minId: Long?,
    @Value("#{jobParameters['maxId']}") maxId: Long?,
  ): JdbcCursorItemReader<Offender> = JdbcCursorItemReaderBuilder<Offender>()
    .name("syncOffenderIdInDefendantReader")
    .dataSource(targetDataSource)
    .fetchSize(3000)
    .sql("${SYNC_OFFENDER_ID_QUERY} WHERE o.legacy_id BETWEEN $minId AND $maxId ORDER BY o.legacy_id ASC")
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
  fun syncOffenderIdInDefendantProcessor(): ItemProcessor<Offender, Defendant> = CompositeItemProcessorBuilder<Offender, Defendant>()
    .delegates(listOf(SyncOffenderIdInDefendantProcessor()))
    .build()

  @Bean
  fun syncOffenderIdInDefendantWriter(): JdbcBatchItemWriter<Defendant> = JdbcBatchItemWriterBuilder<Defendant>()
    .itemSqlParameterSourceProvider(BeanPropertyItemSqlParameterSourceProvider())
    .sql(
      """UPDATE hmpps_court_case_service.defendant SET offender_id = :offenderID WHERE legacy_offender_id = :legacyOffenderID""",
    )
    .dataSource(targetDataSource)
    .assertUpdates(false)
    .build()

  @Bean
  fun syncOffenderIdInDefendantStep(): Step = StepBuilder("syncOffenderIdInDefendantStep", jobRepository)
    .chunk<Offender, Defendant>(batchProperties.chunkSize, transactionManager)
    .reader(syncOffenderIdInDefendantReader(null, null))
    .processor(syncOffenderIdInDefendantProcessor())
    .writer(syncOffenderIdInDefendantWriter())
    .faultTolerant()
    .retry(Throwable::class.java)
    .retryLimit(3)
    .build()

  @Bean
  fun syncOffenderIdInDefendantJobScheduler(dataSource: DataSource, timerJobListener: TimerJobListener) = SyncJobScheduler(
    jobService = syncOffenderIdInDefendantJobService(syncOffenderIdInDefendantJob(timerJobListener)),
    syncJobType = SyncJobType.OFFENDER_ID_DEFENDANT,
    schedulingConfigRepository = SchedulingConfigRepository(JdbcTemplate(dataSource)),
  )
}
