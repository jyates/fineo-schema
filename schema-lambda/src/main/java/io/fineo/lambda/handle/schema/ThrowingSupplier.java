package io.fineo.lambda.handle.schema;

import java.util.function.Supplier;

/**
 *
 */
@FunctionalInterface
public interface ThrowingSupplier<T> {

  T doGet() throws Exception;
}
