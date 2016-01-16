package io.fineo.schema;

/**
 * An implementation of Pair, since javafx is not available AWS Lambda's java
 */
public class Pair<K, V> {

  private K key;
  private V value;

  public Pair(K key, V value) {
    this.key = key;
    this.value = value;
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }

  @Override
  public String toString() {
    return key + "=>" + value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof Pair))
      return false;

    Pair<?, ?> pair = (Pair<?, ?>) o;

    if (getKey() != null ? !getKey().equals(pair.getKey()) : pair.getKey() != null)
      return false;
    return !(getValue() != null ? !getValue().equals(pair.getValue()) : pair.getValue() != null);

  }

  @Override
  public int hashCode() {
    int result = getKey() != null ? getKey().hashCode() : 0;
    result = 31 * result + (getValue() != null ? getValue().hashCode() : 0);
    return result;
  }
}
