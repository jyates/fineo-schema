package io.fineo.schema.avro;

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
}
