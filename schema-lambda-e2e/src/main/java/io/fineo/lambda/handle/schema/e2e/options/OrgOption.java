package io.fineo.lambda.handle.schema.e2e.options;

import com.beust.jcommander.Parameter;

public class OrgOption {

  @Parameter(names = "orgId", description = "Tenant id")
  private String org;

  public String get() {
    return this.org;
  }
}
