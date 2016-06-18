package io.fineo.lambda.handle.schema.e2e.options;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;

public class JsonArgument {

  @Parameter(names = "--json", description = "Path to the json files with the event to send")
  private String json;

  public <T> T get(Class<T> clazz) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(new File(json), clazz);
  }
}
