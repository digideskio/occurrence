package org.gbif.occurrence.es.index.mr;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

public class OccurrenceIndexer {

   public static void main(String[] args) throws IOException {
     JobConf conf = new JobConf();
     conf.setSpeculativeExecution(false);
     conf.set("es.nodes", args[1]);
     conf.set("es.resource", args[2]);
     conf.setOutputFormat(EsOutputFormat.class);
     conf.setMapOutputValueClass(MapWritable.class);
     conf.setMapperClass(OccurrenceAvroMapper.class);
     conf.setJarByClass(OccurrenceIndexer.class);
     FileInputFormat.addInputPath(conf, new Path(args[0]));
     AvroJob.setInputSchema(conf, new Schema.Parser().parse(OccurrenceIndexer.class.getResourceAsStream("/Occurrence.avsc")));
     RunningJob job = JobClient.runJob(conf);
     // Execute job
     job.waitForCompletion();
     System.exit(0);
   }
}
