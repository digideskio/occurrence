package org.gbif.occurrence.es.index.mr;

import org.gbif.occurrence.avro.model.Occurrence;

import java.io.IOException;
import java.lang.reflect.Modifier;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class OccurrenceAvroMapper extends Mapper<AvroKey<Occurrence>, NullWritable, NullWritable,BytesWritable> {

  public static class MyExclusionStrategy implements ExclusionStrategy {
    private final Class<?> typeToSkip;

    private MyExclusionStrategy(Class<?> typeToSkip) {
      this.typeToSkip = typeToSkip;
    }

    public boolean shouldSkipClass(Class<?> clazz) {
      return (clazz == typeToSkip);
    }

    public boolean shouldSkipField(FieldAttributes f) {
      return f.getDeclaredClass() == Schema.class;
    }
  }

  private static Gson GSON = new GsonBuilder()
                              .excludeFieldsWithModifiers(Modifier.STATIC)
                              .addSerializationExclusionStrategy(new MyExclusionStrategy(Schema.class))
                              .setDateFormat("yyyy-MM-dd")
                              .create();


  @Override
  public void map(AvroKey<Occurrence> occurrenceAvro, NullWritable value, Context context) throws IOException, InterruptedException {
    context.write(NullWritable.get(),new BytesWritable(GSON.toJson(occurrenceAvro.datum()).getBytes()));
  }

}
