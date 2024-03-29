package io.fineo.schema;

import com.google.common.base.Joiner;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Stop words/patterns that are not allowed on ingest
 */
public class FineoStopWords {
  private static final Joiner ERROR_MSG_SEPARATOR = Joiner.on("\n");
  public static final String FIELD_PREFIX = "_f";
  private static final String ANYTHING = ".*";
  private static final Pattern _f_PATTERN = Pattern.compile(FIELD_PREFIX + ANYTHING);
  public final static String PREFIX_DELIMITER = "\u00a6\u00a6";
  public static final Pattern
    DRILL_STAR_PREFIX_PATTERN = Pattern.compile("T[0-9]+" + PREFIX_DELIMITER + ANYTHING);

  private ErrorTracker tracker;

  public void recordStart() {
    tracker = new ErrorTracker();
  }

  public void withField(String columnName) {
    if (_f_PATTERN.matcher(columnName).matches()) {
      tracker.add(columnName, "Column starts with _f!");
    } else if (DRILL_STAR_PREFIX_PATTERN.matcher(columnName).matches()) {
      tracker.add(columnName, "Column starts with T<n>" + PREFIX_DELIMITER + "!");
    }
  }

  public void endRecord() {
    tracker.validate();
    tracker = null;
  }


  private class ErrorTracker {
    private Map<String, String> fieldMessage = new HashMap<>();

    public void add(String columnName, String message) {
      this.fieldMessage.put(columnName, message);
    }

    public void validate() {
      if (fieldMessage.size() == 0) {
        return;
      }
      String msg = "Record was invalid! Incorrect fields:\n" + ERROR_MSG_SEPARATOR.join(
        fieldMessage.entrySet().stream().map(e -> e.getKey() + " -> " + e.getValue()).collect
          (Collectors.toList()));
      throw new RuntimeException(msg);
    }
  }
}
