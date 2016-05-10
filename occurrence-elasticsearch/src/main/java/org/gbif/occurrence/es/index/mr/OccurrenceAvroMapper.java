package org.gbif.occurrence.es.index.mr;

import org.gbif.occurrence.avro.model.Occurrence;

import java.io.IOException;

import com.cloudera.com.fasterxml.jackson.annotation.JsonIgnore;
import com.cloudera.com.fasterxml.jackson.annotation.JsonInclude;
import com.cloudera.com.fasterxml.jackson.databind.ObjectMapper;
import com.cloudera.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OccurrenceAvroMapper extends Mapper<AvroKey<Occurrence>, NullWritable, NullWritable,Text> {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceAvroMapper.class);

  public abstract class MixIn {
    @JsonIgnore abstract Schema getSchema(); // we don't need it!
  }

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  static {
    OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    OBJECT_MAPPER.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, Boolean.FALSE);
  }

  @Override
  public void map(AvroKey<Occurrence> occurrenceAvro, NullWritable value, Context context) throws IOException, InterruptedException {
    context.write(NullWritable.get(),new Text(OBJECT_MAPPER.writeValueAsString(occurrenceAvro.datum())));
  }

}
