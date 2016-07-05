package io.fineo.lambda.handle.schema.e2e.options.command;

import com.google.inject.Module;
import io.fineo.lambda.handle.LambdaResponseWrapper;
import io.fineo.lambda.handle.schema.e2e.options.JsonArgument;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

public class BaseCommand<T> {

  private final Function<List<Module>, LambdaResponseWrapper<T, ?, ?>> wrapper;
  private final Class<T> input;

  public BaseCommand(Function<List<Module>, LambdaResponseWrapper<T, ?, ?>> wrapper,
    Class<T> input) {
    this.wrapper = wrapper;
    this.input = input;
  }

  public void run(List<Module> modules, JsonArgument json) throws IOException {
    wrapper.apply(modules).handle(json.get(input), null);
  }
}
