package io.fineo.schema.avro;

import org.schemarepo.Repository;
import org.schemarepo.SchemaEntry;
import org.schemarepo.SchemaValidationException;
import org.schemarepo.Subject;

/**
 * Wraps access to the schema repository and provides helper access to the determine the {@link
 * SchemaDescription} for a given field
 */
public class SchemaAccessor {

  private Repository schemaRepo;

  public SchemaDescription getDescription(String customerId, String fieldNameOrAlias){
    throw new UnsupportedOperationException("not yet implemented");
  }

  public void storeSchema(SchemaDescription description){

  }

  public void updateSchema(SchemaDescription description) throws SchemaValidationException {
    Subject subject = schemaRepo.lookup(description.getSubjectName());
    // ensure the schema
    SchemaEntry entry = subject.register(description.getSchema().toString());
    description.setId(entry.getId());
  }
}
