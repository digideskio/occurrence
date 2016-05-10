package org.gbif.occurrence.es.index.mr;

import org.gbif.occurrence.avro.model.Occurrence;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.codehaus.jackson.Version;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.map.Module;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;

public class OccurrenceAvroMapper extends Mapper<AvroKey<Occurrence>, NullWritable, NullWritable,Text> {


  public abstract class MixIn {
    @JsonIgnore abstract Schema getSchema(); // we don't need it!
  }

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  static {
    OBJECT_MAPPER.getSerializationConfig().setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
    OBJECT_MAPPER.withModule(new Module() {
      @Override
      public String getModuleName() {
        return "ignore schema";
      }

      @Override
      public Version version() {
        return Version.unknownVersion();
      }

      @Override
      public void setupModule(SetupContext context) {
        context.setMixInAnnotations(Occurrence.class,MixIn.class);
      }
    });
  }

  @Override
  public void map(AvroKey<Occurrence> occurrenceAvro, NullWritable value, Context context) throws IOException, InterruptedException {
    context.write(NullWritable.get(),new Text(OBJECT_MAPPER.writeValueAsString(occurrenceAvro.datum())));
  }

}
