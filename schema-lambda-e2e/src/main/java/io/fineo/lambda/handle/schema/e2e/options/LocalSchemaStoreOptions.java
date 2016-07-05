package io.fineo.lambda.handle.schema.e2e.options;

import com.beust.jcommander.Parameter;

/**
 * Options for the location of a locally running (e.g. not in amazon) store
 */
public class LocalSchemaStoreOptions {

  @Parameter(names = "--port", description = "Port on which dynamo is running")
  public int port = -1;

  @Parameter(names = "--host", description = "Hostname where dynamo is running")
  public String host;

  @Parameter(names = "--schema-table", description = "Table where schema information is stored")
  public String table;
}
