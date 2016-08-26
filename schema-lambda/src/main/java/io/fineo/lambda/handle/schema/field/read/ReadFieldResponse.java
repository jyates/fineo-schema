package io.fineo.lambda.handle.schema.field.read;

import java.util.Arrays;

public class ReadFieldResponse {
  public String name;
  public String [] aliases;
  public String type;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String[] getAliases() {
    return aliases;
  }

  public void setAliases(String[] aliases) {
    this.aliases = aliases;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  @Override
  public String toString() {
    return "ReadFieldResponse{" +
           "name='" + name + '\'' +
           ", aliases=" + Arrays.toString(aliases) +
           ", type='" + type + '\'' +
           '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof ReadFieldResponse))
      return false;

    ReadFieldResponse that = (ReadFieldResponse) o;

    if (!getName().equals(that.getName()))
      return false;
    // Probably incorrect - comparing Object[] arrays with Arrays.equals
    if (!Arrays.equals(getAliases(), that.getAliases()))
      return false;
    return getType().equals(that.getType());

  }

  @Override
  public int hashCode() {
    int result = getName().hashCode();
    result = 31 * result + Arrays.hashCode(getAliases());
    result = 31 * result + getType().hashCode();
    return result;
  }
}
