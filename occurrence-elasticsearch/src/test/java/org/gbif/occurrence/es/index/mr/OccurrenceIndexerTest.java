package org.gbif.occurrence.es.index.mr;


import org.gbif.occurrence.avro.model.Occurrence;

import java.io.IOException;
import java.io.File;

import com.google.common.io.Resources;

import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.fest.assertions.api.Assertions.assertThat;

public class OccurrenceIndexerTest {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceIndexerTest.class);
  private static final boolean KEEP_SRC_FILE = false;
  private static final boolean OVERWRITE_EXISTING_DST_FILE = true;
  private static final boolean DO_REFORMAT_HDFS = true;
  private static final String SNAPPY_CODEC = "snappy";

  private static MiniDFSCluster miniDFSCluster;
  private static MiniMRYarnCluster miniMRYarnCluster;

  @BeforeClass
  public static void setUp() throws Exception {
    // Workaround that fixes NPE when trying to start MiniMRCluster.
    // See http://grepalex.com/2012/10/20/hadoop-unit-testing-with-minimrcluster/
    System.setProperty("hadoop.log.dir", System.getProperty("java.io.tmpdir") + "/minimrcluster-logs");
    initMiniDFSCluster();
    initMiniMRYarnCluster(miniDFSCluster.getFileSystem().getUri().toString());
  }

  @AfterClass
  public static void tearDown() {
    if (miniMRYarnCluster != null) {
      miniMRYarnCluster.stop();
      miniMRYarnCluster = null;
    }
    if (miniDFSCluster != null) {
      miniDFSCluster.shutdown();
      miniDFSCluster = null;
    }
  }



  @Test
  public void testOccurrenceIndexing() throws IOException, InterruptedException, ClassNotFoundException {
    // given
    Path inputPath = new Path("testing/occurrence/input");
    Path outputPath = new Path("testing/occurrence/output");

    Configuration configuration = miniMRYarnCluster.getConfig();
    configuration.set(MRConfig.MASTER_ADDRESS, "local");
    Job job = Job.getInstance(configuration);
    updateJobConfiguration(job, inputPath, outputPath);
    upload("avro/occurrence.avro", inputPath);

    // when

    job.waitForCompletion(true);

    // then
    assertThat(job.isSuccessful()).isTrue();
  }

  private void updateJobConfiguration(Job conf, Path inputPath, Path outputPath) throws IOException {
    conf.setJobName("occurrence-es-indexing");

    conf.setInputFormatClass(AvroKeyInputFormat.class);
    AvroJob.setInputKeySchema(conf, Occurrence.getClassSchema());
    conf.setMapOutputKeyClass(NullWritable.class);
    conf.setMapOutputValueClass(Text.class);
    conf.setMapperClass(OccurrenceAvroMapper.class);
    conf.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(conf, inputPath);
    FileOutputFormat.setOutputPath(conf, outputPath);
  }

  private void upload(String resourceFile, Path dstPath) throws IOException {
    LOG.debug("Uploading " + resourceFile + " to " + dstPath);
    Path originalInputFile = new Path(Resources.getResource(resourceFile).getPath());
    Path testInputFile = new Path(dstPath, fileNameOf(resourceFile));
    miniDFSCluster.getFileSystem().copyFromLocalFile(KEEP_SRC_FILE, OVERWRITE_EXISTING_DST_FILE, originalInputFile, testInputFile);
  }

  private static String fileNameOf(String resourceFile) {
    return new Path(resourceFile).getName();
  }

  public static void initMiniDFSCluster() throws IOException {
    Configuration conf = new Configuration();
    File baseDir = new File("./target/hdfs/elasticserachindexing").getAbsoluteFile();
    FileUtil.fullyDelete(baseDir);
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf).numDataNodes(1);
    miniDFSCluster = builder.build();
  }

  public static void initMiniMRYarnCluster(String hdfsUri) {
    miniMRYarnCluster = new MiniMRYarnCluster(OccurrenceIndexerTest.class.getName(), 1);
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", hdfsUri);   // use HDFS
    //conf.set(MRJobConfig.MR_AM_STAGING_DIR, getPathToOutputDirectory()+"/tmp-mapreduce");
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, "/apps_staging_dir");
    miniMRYarnCluster.init(conf);
    miniMRYarnCluster.start();
  }

}
