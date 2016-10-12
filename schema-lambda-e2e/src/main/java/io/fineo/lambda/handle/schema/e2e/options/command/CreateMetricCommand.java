package io.fineo.lambda.handle.schema.e2e.options.command;

import com.beust.jcommander.Parameters;
import io.fineo.lambda.handle.schema.metric.create.CreateMetric;
import io.fineo.lambda.handle.schema.metric.create.CreateMetricRequestInternal;

@Parameters(commandNames = "createMetric",
            commandDescription = "Create a new metric under an org with the given name and aliases")
public class CreateMetricCommand extends BaseCommand<CreateMetricRequestInternal> {

  public CreateMetricCommand() {
    super(modules -> new CreateMetric(modules), CreateMetricRequestInternal.class);
  }
}
