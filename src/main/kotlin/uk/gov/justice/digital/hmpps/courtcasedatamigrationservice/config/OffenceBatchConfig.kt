package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.config

import org.slf4j.LoggerFactory
import org.springframework.batch.core.Job
import org.springframework.batch.core.SkipListener
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.job.builder.JobBuilder
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
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.transaction.PlatformTransactionManager
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.source.OffenceQueryResult
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.domain.target.Offence
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.OffenceJobListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.listener.TimerJobListener
import uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.processor.OffenceProcessor
import javax.sql.DataSource

@Configuration
@EnableBatchProcessing
class OffenceBatchConfig(
  private val jobRepository: JobRepository,
  private val transactionManager: PlatformTransactionManager,
  @Qualifier("sourceDataSource") private val sourceDataSource: DataSource,
  @Qualifier("targetDataSource") private val targetDataSource: DataSource,
  private val batchProperties: BatchProperties,
) {

  private val log = LoggerFactory.getLogger(OffenceBatchConfig::class.java)

  @Bean
  @StepScope
  fun reader(
    @Value("#{jobParameters['minId']}") minId: Long?,
    @Value("#{jobParameters['maxId']}") maxId: Long?,
  ): JdbcCursorItemReader<OffenceQueryResult> = JdbcCursorItemReaderBuilder<OffenceQueryResult>()
    .name("offenceReader")
    .dataSource(sourceDataSource)
    .sql(
      """SELECT
    o.id, o.fk_hearing_defendant_id, o.offence_code, o.summary, o.title, o.sequence, o.act, o.list_no, 
    o.short_term_custody_predictor_score, o.created, o.created_by, o.last_updated, o.last_updated_by, 
    o.deleted, o.version,

    p.id AS plea_id, p.date AS plea_date, p.value AS plea_value, p.created AS plea_created, 
    p.created_by AS plea_created_by, p.last_updated AS plea_last_updated, 
    p.last_updated_by AS plea_last_updated_by, p.deleted AS plea_deleted, p.version AS plea_version,

    v.id AS verdict_id, v.date AS verdict_date, v.type_description AS verdict_type_description, 
    v.created AS verdict_created, v.created_by AS verdict_created_by, 
    v.last_updated AS verdict_last_updated, v.last_updated_by AS verdict_last_updated_by, 
    v.deleted AS verdict_deleted, v.version AS verdict_version,

    (
        SELECT json_agg(json_build_object(
            'id', jr.id,
            'is_convicted_result', jr.is_convicted_result,
            'judicial_result_type_id', jr.judicial_result_type_id,
            'label', jr.label,
            'result_text', jr.result_text,
            'created', jr.created,
            'created_by', jr.created_by,
            'last_updated', jr.last_updated,
            'last_updated_by', jr.last_updated_by,
            'deleted', jr.deleted,
            'version', jr.version
        ))
        FROM courtcaseservice.judicial_result jr
        WHERE jr.offence_id = o.id
    ) AS judicial_results

FROM courtcaseservice.offence o
LEFT JOIN courtcaseservice.plea p ON o.plea_id = p.id
LEFT JOIN courtcaseservice.verdict v ON o.verdict_id = v.id
WHERE o.id BETWEEN $minId AND $maxId
      """.trimMargin(),
    )
    .rowMapper { rs, _ ->
      OffenceQueryResult(
        id = rs.getInt("id"),
        fkHearingDefendantId = rs.getLong("fk_hearing_defendant_id"),
        summary = rs.getString("summary"),
        title = rs.getString("title"),
        offenceCode = rs.getString("offence_code"),
        sequence = rs.getInt("sequence"),
        act = rs.getString("act"),
        listNo = rs.getInt("list_no"),
        shortTeamCustodyPredictorScore = rs.getInt("short_term_custody_predictor_score"),
        created = rs.getTimestamp("created"),
        createdBy = rs.getString("created_by"),
        lastUpdated = rs.getTimestamp("last_updated"),
        lastUpdatedBy = rs.getString("last_updated_by"),
        deleted = rs.getBoolean("deleted"),
        version = rs.getInt("version"),
        pleaId = rs.getObject("plea_id", Integer::class.java)?.toInt(),
        pleaDate = rs.getTimestamp("plea_date"),
        pleaValue = rs.getString("plea_value"),
        pleaCreated = rs.getTimestamp("plea_created"),
        pleaCreatedBy = rs.getString("plea_created_by"),
        pleaLastUpdated = rs.getTimestamp("plea_last_updated"),
        pleaLastUpdatedBy = rs.getString("plea_last_updated_by"),
        pleaDeleted = rs.getBoolean("plea_deleted"),
        pleaVersion = rs.getInt("plea_version"),
        verdictId = rs.getObject("verdict_id", Integer::class.java)?.toInt(),
        verdictDate = rs.getTimestamp("verdict_date"),
        verdictTypeDescription = rs.getString("verdict_type_description"),
        verdictCreated = rs.getTimestamp("verdict_created"),
        verdictCreatedBy = rs.getString("verdict_created_by"),
        verdictLastUpdated = rs.getTimestamp("verdict_last_updated"),
        verdictLastUpdatedBy = rs.getString("verdict_last_updated_by"),
        verdictDeleted = rs.getBoolean("verdict_deleted"),
        verdictVersion = rs.getInt("verdict_version"),
        judicialResults = rs.getString("judicial_results"),
      )
    }
    .build()

  @Bean
  fun processor(): ItemProcessor<OffenceQueryResult, Offence> = CompositeItemProcessorBuilder<OffenceQueryResult, Offence>()
    .delegates(listOf(OffenceProcessor()))
    .build()

  @Bean
  fun writer(): JdbcBatchItemWriter<Offence> = JdbcBatchItemWriterBuilder<Offence>()
    .itemSqlParameterSourceProvider(BeanPropertyItemSqlParameterSourceProvider())
    .sql(
      """INSERT INTO hmpps_court_case_service.offence (id, code, title, legislation, listing_number, sequence, short_term_custody_predictor_score, plea, verdict, judicial_results, created_at, created_by, updated_at, updated_by, is_deleted, version)
        VALUES (:id, :code, :title, :legislation, :listingNumber, :sequence, :shortTermCustodyPredictorScore, CAST(:plea AS jsonb), CAST(:verdict AS jsonb), CAST(:judicialResults AS jsonb), :createdAt, :createdBy, :updatedAt, :updatedBy, :isDeleted, :version)""",
    )
    .dataSource(targetDataSource)
    .build()

  @Bean
  fun skipListener() = object : SkipListener<OffenceQueryResult, Offence> {
    override fun onSkipInRead(t: Throwable) {
      log.warn("Skipped during read: ${t.message}")
    }

    override fun onSkipInProcess(item: OffenceQueryResult, t: Throwable) {
      log.warn("Skipped during process: ${item.id}, reason: ${t.message}")
    }

    override fun onSkipInWrite(item: Offence, t: Throwable) {
      log.warn("Skipped during write: ${item.id}, reason: ${t.message}")
    }
  }

  @Bean
  fun offenceStep(): Step = StepBuilder("offenceStep", jobRepository)
    .chunk<OffenceQueryResult, Offence>(batchProperties.chunkSize, transactionManager)
    .reader(reader(null, null))
    .processor(processor())
    .writer(writer())
    .listener(skipListener())
    .build()

  @Bean
  fun job(timerJobListener: TimerJobListener, offenceJobListener: OffenceJobListener): Job = JobBuilder("offenceJob", jobRepository)
    .incrementer(RunIdIncrementer())
    .listener(timerJobListener)
    .listener(offenceJobListener)
    .start(offenceStep())
    .build()
}
