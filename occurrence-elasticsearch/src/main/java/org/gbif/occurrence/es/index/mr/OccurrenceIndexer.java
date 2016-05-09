package org.gbif.occurrence.es.index.mr;

import org.gbif.occurrence.avro.model.Occurrence;

import java.io.IOException;

import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

public class OccurrenceIndexer {

   public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
     Configuration conf = new Configuration();
     conf.setBoolean(MRJobConfig.MAP_SPECULATIVE,false);
     conf.set("es.nodes", args[1]);
     conf.set("es.resource", args[2]);
     conf.set("es.input.json", "yes");
     conf.set(AvroJob.INPUT_SCHEMA, Occurrence.getClassSchema().toString());
     conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

     Job job = Job.getInstance(conf,"occurrence-es-indexing");

     job.setInputFormatClass(AvroKeyInputFormat.class);


     job.setOutputKeyClass(NullWritable.class);
     job.setOutputValueClass(MapWritable.class);

     job.setMapperClass(OccurrenceAvroMapper.class);
     job.setOutputFormatClass(EsOutputFormat.class);

     job.setJarByClass(OccurrenceIndexer.class);

     FileInputFormat.setInputPaths(job, new Path(args[0]));

     // Execute job
     System.exit(job.waitForCompletion(true) ? 0:1);
   }
}
