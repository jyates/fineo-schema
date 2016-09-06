package io.fineo.schema.store.timestamp;

import com.google.common.base.Preconditions;
import io.fineo.internal.customer.OrgMetadata;
import io.fineo.schema.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.TemporalAccessor;
import java.util.List;

/**
 * Parse the timestamp with various patterns. First the metric-level patterns are attempted,
 * followed by the org-level patterns. If both of those fail, we fall back to trying to parse the
 * field as a long.
 * <p>
 * Users can either specify names of various time formats, or build their own pattern according
 * to the DateTimeFormatter.
 * </p>
 */
public class MultiPatternTimestampParser implements TimestampParser {

  private static final Logger LOG = LoggerFactory.getLogger(MultiPatternTimestampParser.class);

  public enum TimeFormats {
    // 2011-12-03T10:15:30 @ UTC
    ISO_DATE_TIME(DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.of(ZoneOffset.UTC.getId()))),
    // 2011-12-03T10:15:30+01:00
    ISO_OFFSET_DATE_TIME(DateTimeFormatter.ISO_OFFSET_DATE_TIME),
    //2011-12-03T10:15:30+01:00[Europe/Paris]
    ISO_ZONED_DATE_TIME(DateTimeFormatter.ISO_ZONED_DATE_TIME),
    //2011-12-03T10:15:30Z
    ISO_INSTANT(DateTimeFormatter.ISO_INSTANT),
    // Tue, 3 Jun 2008 11:05:30 GMT
    RFC_1123_DATE_TIME(DateTimeFormatter.RFC_1123_DATE_TIME);

    private final DateTimeFormatter formatter;

    TimeFormats(DateTimeFormatter formatter) {
      this.formatter = formatter;
    }
  }

  private final OrgMetadata org;
  private final List<String> fallbackPattterns;
  private final TimestampFieldExtractor extractor;

  public MultiPatternTimestampParser(OrgMetadata org, List<String> fallbackPattterns,
    TimestampFieldExtractor extractor) {
    this.org = org;
    this.fallbackPattterns = fallbackPattterns;
    this.extractor = extractor;
  }

  @Override
  public Long getTimestamp(Record record) {
    String key = extractor.getTimestampKey(record);
    String value = Preconditions.checkNotNull(record.getStringByField(key),
      "Could not find a timestamp in record: %s", record);
    // start with the metric-level aliases
    Long parsed = parseTime(value, fallbackPattterns);
    // maybe the org level has something
    if (parsed == null) {
      parsed = parseTime(value, org.getTimestampFormats());
    }
    // fall back to just trying to read a long
    if (parsed == null) {
      parsed = Long.valueOf(value);
    }
    return parsed;
  }

  private Long parseTime(String value, Iterable<String> formats) {
    if (formats == null) {
      return null;
    }
    for (String format : formats) {
      DateTimeFormatter formatter = getFormatter(format);
      try {
        TemporalAccessor time = formatter.parse(value);
        return Instant.from(time).toEpochMilli();
      } catch (DateTimeParseException e) {
        continue;
      }
    }
    return null;
  }

  public static DateTimeFormatter getFormatter(String format) {
    try {
      return TimeFormats.valueOf(format.toUpperCase()).formatter;
    } catch (IllegalArgumentException e) {
      // not a predefined format, try something to convert it into a pattern
      return DateTimeFormatter.ofPattern(format).withResolverStyle(ResolverStyle.STRICT);
    }
  }
}
