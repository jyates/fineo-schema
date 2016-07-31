package io.fineo.schema;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestFineoStopWords {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void test_f_field() throws Exception {
    tryFields(true, "_f");
  }

  @Test
  public void test_fWithSuffixfield() throws Exception {
    tryFields(true, "_f1");
  }

  @Test
  public void testFieldWith_f() throws Exception {
    tryFields(false, "a_f1");
  }

  @Test
  public void testDrillTablePrefix() throws Exception {
    tryFields(true, "T0" + FineoStopWords.PREFIX_DELIMITER);
  }

  @Test
  public void testDrillTablePrefixMultipleNumbers() throws Exception {
    tryFields(true, "T10" + FineoStopWords.PREFIX_DELIMITER);
  }

  @Test
  public void testDrillTablePrefixButNonNumber() throws Exception {
    tryFields(false, "Ta" + FineoStopWords.PREFIX_DELIMITER);
  }

  @Test
  public void testDrillTablePrefixButNoNumber() throws Exception {
    tryFields(false, "T" + FineoStopWords.PREFIX_DELIMITER);
  }

  @Test
  public void testMultipleFieldsDoesNotThrowUntilAllFieldsSeen() throws Exception {
    tryFields(true, "a", "1_f1", "_f*", "_T" + FineoStopWords.PREFIX_DELIMITER,
      "T10" + FineoStopWords.PREFIX_DELIMITER);
  }

  private void tryFields(boolean expect, String... names) {
    FineoStopWords words = new FineoStopWords();
    words.recordStart();
    for (String n : names) {
      words.withField(n);
    }
    if(expect){
      thrown.expect(RuntimeException.class);
    }
    words.endRecord();
  }
}
