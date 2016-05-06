package org.gbif.occurrence.es.index.mr;

import org.gbif.occurrence.avro.model.Occurrence;

import java.io.IOException;

import org.apache.avro.mapred.AvroInputFormat;
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

     conf.setJobName("occurrence-es-indexing");

     conf.setInputFormat(AvroInputFormat.class);
     conf.set(AvroJob.INPUT_SCHEMA, Occurrence.getClassSchema().toString());

     //conf.setOutputKeyClass(NullWritable.class);
     conf.setMapOutputValueClass(MapWritable.class);


     conf.setMapperClass(OccurrenceAvroMapper.class);
     conf.setOutputFormat(EsOutputFormat.class);

     conf.setJarByClass(OccurrenceIndexer.class);
     FileInputFormat.addInputPath(conf, new Path(args[0]));
     RunningJob job = JobClient.runJob(conf);
     // Execute job
     job.waitForCompletion();
     System.exit(0);
   }
}
