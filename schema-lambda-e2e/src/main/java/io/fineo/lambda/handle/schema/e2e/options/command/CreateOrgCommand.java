package io.fineo.lambda.handle.schema.e2e.options.command;

import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.google.inject.Module;
import io.fineo.lambda.handle.schema.create.CreateOrg;
import io.fineo.lambda.handle.schema.create.CreateOrgRequest;
import io.fineo.lambda.handle.schema.e2e.options.OrgOption;

/**
 * Options for the {@link io.fineo.lambda.handle.schema.metric.create.CreateMetricHandler}
 */
@Parameters(commandNames = "createOrg",
            commandDescription = "Create a new org with the given org id")
public class CreateOrgCommand extends BaseCommand<CreateOrgRequest> {

  public CreateOrgCommand(){
    super(modules -> new CreateOrg(modules), CreateOrgRequest.class);
  }
}
