package io.fineo.lambda.handle.schema.e2e.options.command;

import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import io.fineo.lambda.handle.schema.e2e.options.MetricOption;
import io.fineo.lambda.handle.schema.e2e.options.OrgOption;

@Parameters(commandNames = "createMetric",
            commandDescription = "Create a new metric under an org with the given name and aliases")
public class CreateMetricOptions {
  @ParametersDelegate
  public OrgOption org = new OrgOption();

  @ParametersDelegate
  public MetricOption metric = new MetricOption();
}
