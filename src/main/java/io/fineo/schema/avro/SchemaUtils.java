package io.fineo.schema.avro;

import com.google.common.base.Joiner;
import io.fineo.internal.customer.metric.MetricMetadata;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 *
 */
public class SchemaUtils {

  private static final String NS_SEPARATOR = ".";
  private static final Joiner NS_JOINER = Joiner.on(NS_SEPARATOR);
  public static String BASE_CUSTOMER_NAMESPACE = "io.fineo.cust";

  public static String getCustomerNamespace(String customerid) {
    customerid = cleanNsName(customerid);
    return NS_JOINER.join(BASE_CUSTOMER_NAMESPACE, customerid);
  }

  public static String getCustomerSchemaFullName(CharSequence orgId, CharSequence cannonicalname) {
    String base = getCustomerNamespace(String.valueOf(orgId));
    return NS_JOINER.join(base, cleanNsName(String.valueOf(cannonicalname)));
  }

  private static String cleanNsName(String name){
    name = stripLeadingPeriods(name);
    return stripTrailingPeriods(name);
  }

  private static String stripTrailingPeriods(String name) {
    while (name.endsWith(NS_SEPARATOR)) {
      name = name.substring(0, name.lastIndexOf(NS_SEPARATOR));
    }
    return name;
  }

  private static String stripLeadingPeriods(String customerid) {
    while (customerid.startsWith(NS_SEPARATOR)) {
      customerid = customerid.replaceFirst("[" + NS_SEPARATOR + "]", "");
    }
    return customerid;
  }

  public static Schema parseSchema(CharSequence schema, CharSequence name) {
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

    return bos.toString();
  }

  public static <T> T readFromString(String encoded, Schema schema) throws IOException {
    Decoder dec = DecoderFactory.get().jsonDecoder(MetricMetadata.getClassSchema(), encoded);
    SpecificDatumReader reader = new SpecificDatumReader(schema);
    return (T) reader.read(null, dec);
  }
}
