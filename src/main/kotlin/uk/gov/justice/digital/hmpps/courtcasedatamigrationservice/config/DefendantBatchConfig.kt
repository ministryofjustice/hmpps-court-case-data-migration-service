package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.config

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
    .sql(
      """
        SELECT d.id
FROM courtcaseservice.defendant d
WHERE d.id BETWEEN $minId AND $maxId
      """.trimMargin(),
    )
    .rowMapper { rs, _ ->
      DefendantQueryResult(
        id = rs.getInt("id"),
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
      """INSERT INTO hmpps_court_case_service.defendant (id, master_defendant_id, is_manual_update, crn, cro_number, is_youth, tsv_name, pnc_id, is_proceedings_concluded, cpr_uuid, is_offender_confirmed, person, created_at, created_by, updated_at, updated_by, is_deleted, version)
        VALUES (:id, :masterDefendantId, :isManualUpdate, :crn, :croNumber, :isYouth, :tsvName, :pncId, :isProceedingsConcluded, :cprUuid, :isOffenderConfirmed, CAST(:person AS jsonb), :createdAt, :createdBy, :updatedAt, :updatedBy, :isDeleted, :version)""",
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
    sourceRowCountQuery = "SELECT COUNT(*) FROM courtcaseservice.defendant d",
    targetRowCountQuery = "SELECT COUNT(*) FROM hmpps_court_case_service.defendant d",
  )

  @Bean
  fun defendantJob(timerJobListener: TimerJobListener): Job = JobBuilder("defendantJob", jobRepository)
    .incrementer(RunIdIncrementer())
    .listener(timerJobListener)
    .listener(defendantRowCountListener())
    .start(defendantStep())
    .build()

  @Bean(name = ["defendantJobService"])
  fun defendantJobService(@Qualifier("defendantJob") defendantJob: Job): JobService = JobService(
    jobLauncher = jobLauncher,
    job = defendantJob,
    sourceJdbcTemplate = JdbcTemplate(sourceDataSource),
    batchSize = 10,
    minQuery = "SELECT MIN(id) FROM courtcaseservice.defendant",
    maxQuery = "SELECT MAX(id) FROM courtcaseservice.defendant",
    jobName = "Defendant",
  )

  @Bean
  fun defendantJobScheduler(dataSource: DataSource, timerJobListener: TimerJobListener) = JobScheduler(jobService = defendantJobService(defendantJob(timerJobListener)), dataSource = dataSource, jobType = JobType.DEFENDANT)
}
