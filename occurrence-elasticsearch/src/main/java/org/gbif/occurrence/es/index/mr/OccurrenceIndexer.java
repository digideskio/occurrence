    package org.gbif.occurrence.es.index.mr;

    import org.gbif.occurrence.avro.model.Occurrence;

    import java.io.IOException;

    import org.apache.avro.mapreduce.AvroJob;
    import org.apache.avro.mapreduce.AvroKeyInputFormat;
    import org.apache.hadoop.conf.Configuration;
    import org.apache.hadoop.conf.Configured;
    import org.apache.hadoop.fs.Path;
    import org.apache.hadoop.io.BytesWritable;
    import org.apache.hadoop.io.NullWritable;
    import org.apache.hadoop.mapreduce.Job;
    import org.apache.hadoop.mapreduce.MRJobConfig;
    import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
    import org.apache.hadoop.util.Tool;
    import org.apache.hadoop.util.ToolRunner;
    import org.elasticsearch.hadoop.mr.EsOutputFormat;

    public class OccurrenceIndexer extends Configured implements Tool {

      public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new OccurrenceIndexer(), args));
      }

      @Override
      public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = getConf();
        conf.setBoolean(MRJobConfig.MAP_SPECULATIVE,false);
        conf.set("es.nodes", args[1]);
        conf.set("es.port", "9200");
        conf.set("es.resource", args[2]);
        conf.set("es.input.json", "yes");
        conf.set("es.mapping.id", "key");
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
        conf.setBoolean(MRJobConfig.MAPREDUCE_TASK_CLASSPATH_PRECEDENCE, true);

        Job job = Job.getInstance(conf,"occurrence-es-indexing");
        job.setUserClassesTakesPrecedence(true);

        job.setInputFormatClass(AvroKeyInputFormat.class);
        AvroJob.setInputKeySchema(job, Occurrence.getClassSchema());
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setMapperClass(OccurrenceAvroMapper.class);
        job.setOutputFormatClass(EsOutputFormat.class);

        job.setJarByClass(OccurrenceIndexer.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // Execute job
        return (job.waitForCompletion(true) ? 0:1);
      }
    }
