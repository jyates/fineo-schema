package io.fineo.lambda.handle.schema.e2e.options.command;

import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import io.fineo.lambda.handle.schema.e2e.options.OrgOption;

/**
 * Options for the {@link io.fineo.lambda.handle.schema.metric.create.CreateMetricHandler}
 */
@Parameters(commandNames = "createOrg", commandDescription = "Create a new org with the given org id")
public class CreateOrgOptions {

  @ParametersDelegate
  public OrgOption org = new OrgOption();
}
