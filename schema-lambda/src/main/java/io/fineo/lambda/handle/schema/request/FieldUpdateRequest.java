package io.fineo.lambda.handle.schema.request;

public class FieldUpdateRequest extends FieldRequest {
  private String[] aliases;

  public String[] getAliases() {
    return aliases;
  }

  public void setAliases(String[] aliases) {
    this.aliases = aliases;
  }
}
