package uk.gov.justice.digital.hmpps.courtcasedatamigrationservice

import java.util.UUID

object TestUtils {

  fun isValueUUID(uuid: String): Boolean = try {
    UUID.fromString(uuid)
    true
  } catch (e: IllegalArgumentException) {
    false
  }
}
