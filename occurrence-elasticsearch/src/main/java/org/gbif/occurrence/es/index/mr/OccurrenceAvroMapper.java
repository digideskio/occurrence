package org.gbif.occurrence.es.index.mr;

import org.gbif.occurrence.avro.model.Occurrence;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OccurrenceAvroMapper  extends Mapper<AvroKey<Occurrence>, NullWritable, NullWritable,Text> {

  abstract class MixIn {
    @JsonIgnore abstract Schema getSchema(); // we don't need it!
  }

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  static {
    OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    OBJECT_MAPPER.configure(SerializationFeature.WRITE_NULL_MAP_VALUES,Boolean.FALSE);
    OBJECT_MAPPER.addMixIn(Occurrence.class,MixIn.class);
  }

  @Override
  public void map(AvroKey<Occurrence> occurrenceAvro, NullWritable value, Context context) throws IOException, InterruptedException {
    context.write(NullWritable.get(),new Text(OBJECT_MAPPER.writeValueAsString(occurrenceAvro.datum())));
  }

}
