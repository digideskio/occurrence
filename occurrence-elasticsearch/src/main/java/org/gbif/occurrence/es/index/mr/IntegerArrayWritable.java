package org.gbif.occurrence.es.index.mr;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

public class IntegerArrayWritable extends ArrayWritable {

  public IntegerArrayWritable() {
    super(IntWritable.class);
  }
  public IntegerArrayWritable(IntWritable[] values) {
    super(IntWritable.class, values);
  }
}
