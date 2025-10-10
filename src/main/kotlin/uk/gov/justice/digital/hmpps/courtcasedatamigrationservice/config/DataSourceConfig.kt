package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.config

import com.zaxxer.hikari.HikariDataSource
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.jdbc.core.JdbcTemplate
import javax.sql.DataSource

@Configuration
class DataSourceConfig {

  @Primary
  @Bean(name = ["dataSourceProperties"])
  @ConfigurationProperties("spring.datasource")
  fun batchDataSourceProperties() = DataSourceProperties()

  @Primary
  @Bean(name = ["dataSource"])
  fun batchDataSource(
    @Qualifier("dataSourceProperties") properties: DataSourceProperties,
  ): DataSource = properties.initializeDataSourceBuilder().build()

  @Bean
  @ConfigurationProperties("app.datasource.source")
  fun sourceDataSourceProperties() = DataSourceProperties()

  @Bean
  fun sourceDataSource(
    @Qualifier("sourceDataSourceProperties") properties: DataSourceProperties,
  ): DataSource {
    val dataSource = properties.initializeDataSourceBuilder().type(HikariDataSource::class.java).build()
    dataSource.isAutoCommit = false
    return dataSource
  }

  @Bean
  @ConfigurationProperties("app.datasource.target")
  fun targetDataSourceProperties() = DataSourceProperties()

  @Bean
  fun targetDataSource(
    @Qualifier("targetDataSourceProperties") properties: DataSourceProperties,
  ): DataSource = properties.initializeDataSourceBuilder().build()

  @Bean
  @Qualifier("sourceJdbcTemplate")
  fun sourceJdbcTemplate(@Qualifier("sourceDataSource") dataSource: DataSource): JdbcTemplate = JdbcTemplate(dataSource)

  @Bean
  @Qualifier("targetJdbcTemplate")
  fun targetJdbcTemplate(@Qualifier("targetDataSource") dataSource: DataSource): JdbcTemplate = JdbcTemplate(dataSource)
}
