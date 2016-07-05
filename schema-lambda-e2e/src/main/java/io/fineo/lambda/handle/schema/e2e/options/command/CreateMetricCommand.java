package io.fineo.lambda.handle.schema.e2e.options.command;

import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import io.fineo.lambda.handle.schema.create.CreateOrg;
import io.fineo.lambda.handle.schema.create.CreateOrgRequest;
import io.fineo.lambda.handle.schema.e2e.options.MetricOption;
import io.fineo.lambda.handle.schema.e2e.options.OrgOption;
import io.fineo.lambda.handle.schema.metric.create.CreateMetric;
import io.fineo.lambda.handle.schema.metric.create.CreateMetricRequest;

@Parameters(commandNames = "createMetric",
            commandDescription = "Create a new metric under an org with the given name and aliases")
public class CreateMetricCommand extends BaseCommand<CreateMetricRequest> {

  public CreateMetricCommand(){
    super(modules -> new CreateMetric(modules), CreateMetricRequest.class);
  }
}
