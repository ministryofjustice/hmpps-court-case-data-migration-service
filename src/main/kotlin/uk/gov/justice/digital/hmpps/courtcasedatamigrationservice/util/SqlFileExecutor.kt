package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice.util

import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.core.io.ClassPathResource
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component
import javax.sql.DataSource

@Component
class SqlFileExecutor(@Qualifier("targetDataSource") private val targetDataSource: DataSource) {

  fun <T> executeScalar(sqlFilePath: String, resultType: Class<T>): T? {
    val sql = readSqlFile(sqlFilePath)
    return JdbcTemplate(targetDataSource).queryForObject(sql, resultType)
  }

  fun executeVoid(sqlFilePath: String) {
    val sql = readSqlFile(sqlFilePath)
    JdbcTemplate(targetDataSource).execute(sql)
  }

  fun executeQuery(sqlFilePath: String): List<Map<String, Any>> {
    val sql = readSqlFile(sqlFilePath)
    return JdbcTemplate(targetDataSource).queryForList(sql)
  }

  private fun readSqlFile(path: String): String = ClassPathResource(path).inputStream.bufferedReader().use { it.readText() }
}
