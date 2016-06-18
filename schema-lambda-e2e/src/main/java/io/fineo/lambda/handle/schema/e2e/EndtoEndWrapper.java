package io.fineo.lambda.handle.schema.e2e;

import com.beust.jcommander.JCommander;
import com.google.inject.Module;
import io.fineo.lambda.configure.PropertiesModule;
import io.fineo.lambda.dynamo.DynamoTestConfiguratorModule;
import io.fineo.lambda.handle.LambdaResponseWrapper;
import io.fineo.lambda.handle.schema.create.CreateOrg;
import io.fineo.lambda.handle.schema.create.CreateOrgRequest;
import io.fineo.lambda.handle.schema.e2e.module.FakeAwsCredentialsModule;
import io.fineo.lambda.handle.schema.e2e.options.JsonArgument;
import io.fineo.lambda.handle.schema.e2e.options.LocalSchemaStoreOptions;
import io.fineo.lambda.handle.schema.e2e.options.command.CreateMetricOptions;
import io.fineo.lambda.handle.schema.e2e.options.command.CreateOrgOptions;
import io.fineo.lambda.handle.schema.field.delete.DeleteField;
import io.fineo.lambda.handle.schema.field.delete.DeleteFieldRequest;
import io.fineo.lambda.handle.schema.field.update.UpdateField;
import io.fineo.lambda.handle.schema.field.update.UpdateFieldRequest;
import io.fineo.lambda.handle.schema.inject.SchemaModulesUtil;
import io.fineo.lambda.handle.schema.inject.SchemaStoreModule;
import io.fineo.lambda.handle.schema.metric.create.CreateMetric;
import io.fineo.lambda.handle.schema.metric.create.CreateMetricRequest;
import io.fineo.lambda.handle.schema.metric.delete.DeleteMetric;
import io.fineo.lambda.handle.schema.metric.delete.DeleteMetricRequest;
import io.fineo.lambda.handle.schema.metric.field.AddFieldToMetric;
import io.fineo.lambda.handle.schema.metric.field.AddFieldToMetricRequest;
import io.fineo.lambda.handle.schema.metric.update.UpdateMetric;
import io.fineo.lambda.handle.schema.metric.update.UpdateMetricRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;

/**
 * Wrapper for schema functions so we can leverage them directly like they would be called
 * from an external API.
 */
public class EndtoEndWrapper {

  public static void main(String[] args) throws IOException {
    LocalSchemaStoreOptions localStore = new LocalSchemaStoreOptions();
    JsonArgument json = new JsonArgument();
    JCommander jc = new JCommander(new Object[]{localStore, json});

    CreateOrgOptions co = new CreateOrgOptions();
    jc.addCommand("createOrg", co);
    CreateMetricOptions cm = new CreateMetricOptions();
    jc.addCommand("createMetric", cm);

    jc.parse(args);

    List<Module> schemaStore = getScheamStoreModules(localStore);
    switch (jc.getParsedCommand()) {
      case "createOrg":
        handle(s -> new CreateOrg(s), CreateOrgRequest.class, schemaStore, json);
        break;
      case "createMetric":
        handle(s -> new CreateMetric(s), CreateMetricRequest.class, schemaStore, json);
        break;
      case "deleteMetric":
        handle(s -> new DeleteMetric(s), DeleteMetricRequest.class, schemaStore, json);
        break;
      case "updateMetric":
        handle(s -> new UpdateMetric(s), UpdateMetricRequest.class, schemaStore, json);
        break;
      case "addField":
        handle(s -> new AddFieldToMetric(s), AddFieldToMetricRequest.class, schemaStore, json);
        break;
      case "deleteField":
        handle(s -> new DeleteField(s), DeleteFieldRequest.class, schemaStore, json);
        break;
      case "updateField":
        handle(s -> new UpdateField(s), UpdateFieldRequest.class, schemaStore, json);
        break;
      default:
        throw new IllegalArgumentException("Don't know how to handle: " + jc.getParsedCommand());
    }
  }

  public static <T> void handle(Function<List<Module>, LambdaResponseWrapper<T, ?, ?>> wrapper,
    Class<T> input, List<Module> modules, JsonArgument json) throws IOException {
    wrapper.apply(modules).handle(json.get(input), null);
  }

  private static List<Module> getScheamStoreModules(LocalSchemaStoreOptions localStore) {
    List<Module> modules = new ArrayList<>();
    // fake support for dynamo
    modules.add(new FakeAwsCredentialsModule());
    modules.add(new DynamoTestConfiguratorModule());

    // properties to support the build
    Properties props = new Properties();
    props.setProperty(DynamoTestConfiguratorModule.DYNAMO_URL_FOR_TESTING,
      localStore.host + ":" + localStore.port);
    props.setProperty(SchemaStoreModule.SCHEMA_UPDATE_RETRIES, "1");
    props.setProperty(SchemaStoreModule.DYNAMO_SCHEMA_STORE_TABLE, "schema-table_" + UUID.randomUUID());
    props.setProperty(SchemaStoreModule.DYNAMO_READ_LIMIT, "1");
    props.setProperty(SchemaStoreModule.DYNAMO_WRITE_LIMIT, "1");
    modules.add(new PropertiesModule(props));

    // core, non-production modules. These are used in combination with the modules above to
    // generate the output state
    SchemaModulesUtil.addBaseSchemaModules(modules);
    return modules;
  }
}
