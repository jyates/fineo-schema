package io.fineo.lambda.handle.schema.e2e.options.command;

import com.beust.jcommander.Parameters;
import io.fineo.lambda.handle.schema.metric.field.AddFieldToMetric;
import io.fineo.lambda.handle.schema.metric.field.AddFieldToMetricRequestInternal;

/**
 *
 */
@Parameters(commandNames = "addField",
            commandDescription = "Add a field to an existing metric")
public class AddFieldCommand extends BaseCommand<AddFieldToMetricRequestInternal> {

  public AddFieldCommand(){
    super(modules -> new AddFieldToMetric(modules), AddFieldToMetricRequestInternal.class);
  }
}
