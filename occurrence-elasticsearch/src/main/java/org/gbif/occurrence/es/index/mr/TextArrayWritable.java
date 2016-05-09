package org.gbif.occurrence.es.index.mr;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TextArrayWritable extends ArrayWritable {

  public TextArrayWritable() {
    super(Text.class);
  }

  public TextArrayWritable(Text[] values) {
    super(Text.class, values);
  }

  public TextArrayWritable(String[] values) {
    super(values);
  }

  public Text[] get() {
    Writable[] writables = super.get();
    Text[] texts = new Text[writables.length];
    for(int i=0; i<writables.length; ++i)
      texts[i] = (Text)writables[i];
    return texts;
  }
}
