package io.fineo.schema.avro;

import io.fineo.internal.customer.metric.MetricMetadata;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 *
 */
public class SchemaUtils {

  public static String BASE_CUSTOMER_NAMESPACE = "io.fineo.cust.";

  public static String getCustomerNamespace(String customerid) {
    customerid = stripLeadingPeriods(customerid);
    return BASE_CUSTOMER_NAMESPACE + customerid;
  }

  private static String stripLeadingPeriods(String customerid) {
    while (customerid.startsWith(".")) {
      customerid = customerid.replaceFirst("[.]", "");
    }
    return customerid;
  }

  public static Schema parseSchema(CharSequence schema, CharSequence name){
    Schema.Parser parser = new Schema.Parser();
    parser.parse(String.valueOf(schema));
    return parser.getTypes().get(name);
  }

  public static <T extends SpecificRecordBase> String toString(T record) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Schema schema = record.getSchema();
    SpecificDatumWriter<T> userDatumWriter = new SpecificDatumWriter<>(schema);
    Encoder enc = EncoderFactory.get().jsonEncoder(schema, bos);
    userDatumWriter.write(record, enc);
    enc.flush();
    bos.close();

    return new String(bos.toByteArray());
  }

  public static <T> T readFromString(String encoded, Schema schema) throws IOException {
    Decoder dec = DecoderFactory.get().jsonDecoder(MetricMetadata.getClassSchema(), encoded);
    SpecificDatumReader reader = new SpecificDatumReader(schema);
    return (T) reader.read(null, dec);
  }
}
