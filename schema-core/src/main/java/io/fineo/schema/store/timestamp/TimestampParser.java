package io.fineo.schema.store.timestamp;

import io.fineo.schema.Record;

/**
 * Parse the timestamp from a record
 */
public interface TimestampParser {

  Long getTimestamp(Record record);
}
