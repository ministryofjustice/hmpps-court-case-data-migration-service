package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.config

import org.slf4j.LoggerFactory
import org.springframework.batch.core.Job
import org.springframework.batch.core.JobExecutionListener
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
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.JobType
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.DefendantQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Defendant
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.RowCountListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.TimerJobListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.DefendantProcessor
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.scheduler.JobScheduler
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.service.JobService
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.DefendantValidationStrategy
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.tasklet.PostMigrationValidator
import java.time.LocalDate
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

  companion object {
    const val MIN_QUERY = "SELECT MIN(id) FROM courtcaseservice.defendant"
    const val MAX_QUERY = "SELECT MAX(id) FROM courtcaseservice.defendant"
    const val SOURCE_ROW_COUNT_QUERY = "SELECT COUNT(*) FROM courtcaseservice.defendant d"
    const val TARGET_ROW_COUNT_QUERY = "SELECT COUNT(*) FROM hmpps_court_case_service.defendant d"
  }

  @Bean
  @StepScope
  fun defendantReader(
    @Value("#{jobParameters['minId']}") minId: Long?,
    @Value("#{jobParameters['maxId']}") maxId: Long?,
  ): JdbcCursorItemReader<DefendantQueryResult> = JdbcCursorItemReaderBuilder<DefendantQueryResult>()
    .name("defendantReader")
    .dataSource(sourceDataSource)
    .sql(
      """
        SELECT d.id, d.manual_update, d.crn, d.cro, d.name, d.date_of_birth, d.offender_confirmed, d.nationality_1, d.nationality_2, d.sex, d.phone_number, d.address, d.tsv_name, d.pnc, d.cpr_uuid, d.created, d.created_by, d.last_updated, d.last_updated_by, d.deleted, d.version
FROM courtcaseservice.defendant d
WHERE d.id BETWEEN $minId AND $maxId
      """.trimMargin(),
    )
    .rowMapper { rs, _ ->
      DefendantQueryResult(
        id = rs.getInt("id"),
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
        cpr_uuid = rs.getString("cpr_uuid"),
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
      """INSERT INTO hmpps_court_case_service.defendant (id, is_manual_update, crn, cro_number, tsv_name, pnc_id, cpr_uuid, is_offender_confirmed, person, created_at, created_by, updated_at, updated_by, is_deleted, version)
        VALUES (:id, :isManualUpdate, :crn, :croNumber, to_tsvector(:tsvName), :pncId, :cprUuid, :isOffenderConfirmed, CAST(:person AS jsonb), :createdAt, :createdBy, :updatedAt, :updatedBy, :isDeleted, :version)""",
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
    val strategy = DefendantValidationStrategy(
      sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
      targetJdbcTemplate = JdbcTemplate(targetDataSource),
    )
    return PostMigrationValidator(strategy, 25)
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
    sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
    batchSize = 10,
    minQuery = MIN_QUERY,
    maxQuery = MAX_QUERY,
    jobName = "Defendant",
  )

  @Bean
  fun defendantJobScheduler(dataSource: DataSource, timerJobListener: TimerJobListener) = JobScheduler(jobService = defendantJobService(defendantJob(timerJobListener)), dataSource = dataSource, jobType = JobType.DEFENDANT)
}
