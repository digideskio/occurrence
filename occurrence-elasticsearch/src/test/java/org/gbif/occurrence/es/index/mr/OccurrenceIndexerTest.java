package org.gbif.occurrence.es.index.mr;


import org.gbif.occurrence.avro.model.Occurrence;

import java.io.IOException;
import java.io.File;
import java.nio.file.Files;

import com.google.common.io.Resources;

import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.fest.assertions.api.Assertions.assertThat;

public class OccurrenceIndexerTest extends ClusterMapReduceTestCase {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceIndexerTest.class);
  private static final boolean KEEP_SRC_FILE = false;
  private static final boolean OVERWRITE_EXISTING_DST_FILE = true;
  private static final boolean DO_REFORMAT_HDFS = true;
  private static final String SNAPPY_CODEC = "snappy";

  @Override
  protected void setUp() throws Exception {
    // Workaround that fixes NPE when trying to start MiniMRCluster.
    // See http://grepalex.com/2012/10/20/hadoop-unit-testing-with-minimrcluster/
    System.setProperty("hadoop.log.dir", System.getProperty("java.io.tmpdir") + "/minimrcluster-logs");
    startCluster(DO_REFORMAT_HDFS, null);
  }


  @Test
  public void testOccurrenceIndexing() throws IOException {
    // given
    Path inputPath = new Path("testing/occurrence/input");
    Path outputPath = new Path("testing/occurrence/output");

    JobConf conf = createJobConf();
    updateJobConfiguration(conf, inputPath, outputPath);
    upload("avro/occurrence.avro", inputPath);

    // when
    RunningJob job = JobClient.runJob(conf);
    job.waitForCompletion();

    // then
    assertThat(job.isSuccessful()).isTrue();

//    Path[] outputFiles = FileUtil.stat2Paths(getFileSystem().listStatus(outputPath,
 //                                                                       new Utils.OutputFileUtils.OutputFilesFilter()));
//    assertThat(outputFiles.length).isEqualTo(1);

//    Path outputFile = outputFiles[0];
//    assertThatAvroOutputIsIdentical("avro/occurrence.avro", outputFile);
  }

  private void updateJobConfiguration(JobConf conf, Path inputPath, Path outputPath) {
    conf.setJobName("occurrence-es-indexing");

    conf.setInputFormat(AvroInputFormat.class);
    conf.set(AvroJob.INPUT_SCHEMA, Occurrence.getClassSchema().toString());

    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputValueClass(MapWritable.class);
                         //AvroJob.setOutputSchema(conf, MapWritable.class);

    conf.setMapperClass(OccurrenceAvroMapper.class);

    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(conf, inputPath);
    FileOutputFormat.setOutputPath(conf, outputPath);

    // Disable JDK 7's new bytecode verifier which requires the need for stack frames.  This is required when
    // running this code via JDK 7.  Otherwise our map/reduce tasks spawned by MiniMRCluster will fail because of
    // "Error: Expecting a stackmap frame at branch target [...]"
    //
    // See also:
    // http://chrononsystems.com/blog/java-7-design-flaw-leads-to-huge-backward-step-for-the-jvm
    // http://stackoverflow.com/questions/8958267/java-lang-verifyerror-expecting-a-stackmap-frame
    //
    conf.set("mapred.child.java.opts", "-XX:-UseSplitVerifier");
  }

  private void upload(String resourceFile, Path dstPath) throws IOException {
    LOG.debug("Uploading " + resourceFile + " to " + dstPath);
    Path originalInputFile = new Path(Resources.getResource(resourceFile).getPath());
    Path testInputFile = new Path(dstPath, fileNameOf(resourceFile));
    getFileSystem().copyFromLocalFile(KEEP_SRC_FILE, OVERWRITE_EXISTING_DST_FILE, originalInputFile, testInputFile);
  }

  private String fileNameOf(String resourceFile) {
    return new Path(resourceFile).getName();
  }

  private void assertThatAvroOutputIsIdentical(String expectedOutputResourceFile, Path outputFile)
    throws IOException {
    LOG.debug("Comparing contents of " + expectedOutputResourceFile + " and " + outputFile);
    Path expectedOutput = new Path(Resources.getResource(expectedOutputResourceFile).getPath());
    Path tmpLocalOutput = createTempLocalPath();
    getFileSystem().copyToLocalFile(outputFile, tmpLocalOutput);
    try {
      assertThat(AvroDataComparer.haveIdenticalContents(expectedOutput, tmpLocalOutput)).isTrue();
    }
    finally {
      delete(tmpLocalOutput);
    }
  }

  private static Path createTempLocalPath() throws IOException {
    java.nio.file.Path path = Files.createTempFile("test-tweetcount-actual-output-", ".avro");
    // delete the temp file immediately -- we are just interested in the generated filename
    path.toFile().delete();
    return new Path(path.toAbsolutePath().toString());
  }

  private static void delete(Path path) throws IOException {
    new File(path.toString()).delete();
  }


}
