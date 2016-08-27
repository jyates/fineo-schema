package io.fineo.lambda.handle.schema.e2e;

import com.beust.jcommander.JCommander;
import com.google.inject.Module;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.dynamo.DynamoTestConfiguratorModule;
import io.fineo.lambda.handle.schema.e2e.module.FakeAwsCredentialsModule;
import io.fineo.lambda.handle.schema.e2e.options.JsonArgument;
import io.fineo.lambda.handle.schema.e2e.options.LocalSchemaStoreOptions;
import io.fineo.lambda.handle.schema.e2e.options.command.AddFieldCommand;
import io.fineo.lambda.handle.schema.e2e.options.command.BaseCommand;
import io.fineo.lambda.handle.schema.e2e.options.command.CreateMetricCommand;
import io.fineo.lambda.handle.schema.e2e.options.command.CreateOrgCommand;
import io.fineo.lambda.handle.schema.inject.SchemaModulesUtil;
import io.fineo.lambda.handle.schema.inject.SchemaStoreModule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Wrapper for schema functions so we can leverage them directly like they would be called
 * from an external API.
 */
public class EndtoEndWrapper {

  public static void main(String[] args) throws IOException {
    LocalSchemaStoreOptions localStore = new LocalSchemaStoreOptions();
    JsonArgument json = new JsonArgument();
    JCommander jc = new JCommander(new Object[]{localStore, json});

    jc.addCommand("createOrg", new CreateOrgCommand());
    jc.addCommand("createMetric", new CreateMetricCommand());
    jc.addCommand("addField", new AddFieldCommand());

    jc.parse(args);

    List<Module> schemaStore = getSchemaStoreModules(localStore);
    String cmd = jc.getParsedCommand();
    BaseCommand command = (BaseCommand) jc.getCommands().get(cmd).getObjects().get(0);
    command.run(schemaStore, json);
  }

  private static List<Module> getSchemaStoreModules(LocalSchemaStoreOptions localStore) {
    List<Module> modules = new ArrayList<>();
    // fake support for dynamo
    modules.add(new FakeAwsCredentialsModule());
    modules.add(new DynamoTestConfiguratorModule());

    // properties to support the build
    Properties props = new Properties();
    props.setProperty(DynamoTestConfiguratorModule.DYNAMO_URL_FOR_TESTING,
      "http://"+localStore.host + ":" + localStore.port);
    props.setProperty(SchemaStoreModule.SCHEMA_UPDATE_RETRIES, "1");
    props.setProperty(SchemaStoreModule.DYNAMO_SCHEMA_STORE_TABLE, localStore.table);
    modules.add(new PropertiesModule(props));

    // core, non-production modules. These are used in combination with the modules above to
    // generate the output state
    SchemaModulesUtil.addBaseSchemaModules(modules);
    return modules;
  }
}
