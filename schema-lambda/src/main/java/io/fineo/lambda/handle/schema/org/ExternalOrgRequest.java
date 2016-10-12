package io.fineo.lambda.handle.schema.org;

import io.fineo.client.model.schema.ReadSchemaManagement;
import io.fineo.client.model.schema.SchemaManagementRequest;
import io.fineo.lambda.handle.schema.request.OrgRequest;

/**
 *
 */
public class ExternalOrgRequest extends OrgRequest{

  private String type;
  private SchemaManagementRequest patch;
  private ReadSchemaManagement get;

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public SchemaManagementRequest getPatch() {
    return patch;
  }

  public void setPatch(SchemaManagementRequest patch) {
    this.patch = patch;
  }

  public ReadSchemaManagement getGet() {
    return get;
  }

  public void setGet(ReadSchemaManagement get) {
    this.get = get;
  }
}
