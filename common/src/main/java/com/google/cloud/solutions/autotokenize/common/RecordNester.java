package com.google.cloud.solutions.autotokenize.common;

import com.google.auto.value.AutoValue;
import com.google.cloud.solutions.autotokenize.AutoTokenizeMessages.FlatRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

/** Convert a FlatRecord to Generic Record and set Beam schema accordingly. */
@AutoValue
public abstract class RecordNester extends PTransform<PCollection<FlatRecord>, PCollection<GenericRecord>> {

  abstract String schemaJson();

  public static RecordNester forSchemaJson(String schemaJson) {
    return new AutoValue_RecordNester(schemaJson);
  }

  public static RecordNester forSchema(Schema outputSchema) {
    return forSchemaJson(outputSchema.toString());
  }

  @Override
  public PCollection<GenericRecord> expand(PCollection<FlatRecord> flatRecords) {
    return flatRecords.apply("ReNestRecords", MapElements.via(new RecordNesterFn(schemaJson())))
      .setCoder(AvroUtils.schemaCoder(GenericRecord.class, new Schema.Parser().parse(schemaJson())));
  }


  private static class RecordNesterFn extends SimpleFunction<FlatRecord, GenericRecord> {

    private final String outputSchemaString;

    private RecordNesterFn(String outputSchemaString) {
      this.outputSchemaString = outputSchemaString;
    }

    @Override
    public GenericRecord apply(FlatRecord flatRecord) {
      Schema outputSchema = new Schema.Parser().parse(outputSchemaString);
      return RecordUnflattener.forSchema(outputSchema).unflatten(flatRecord);
    }
  }
}
