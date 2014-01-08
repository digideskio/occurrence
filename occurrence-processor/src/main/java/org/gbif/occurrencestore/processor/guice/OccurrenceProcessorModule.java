package org.gbif.occurrencestore.processor.guice;

import org.gbif.api.service.occurrence.OccurrenceService;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.guice.PostalServiceModule;
import org.gbif.occurrencestore.persistence.api.FragmentPersistenceService;
import org.gbif.occurrencestore.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrencestore.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrencestore.persistence.api.VerbatimOccurrencePersistenceService;
import org.gbif.occurrencestore.persistence.guice.OccurrencePersistenceModule;
import org.gbif.occurrencestore.processor.zookeeper.ZookeeperConnector;
import org.gbif.service.guice.PrivateServiceModule;

import java.util.Properties;

import com.google.inject.Provides;
import com.google.inject.name.Named;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;

/**
 * The Guice module that configures everything needed for the processing to start up. See the README for needed
 * properties. Only needed when using the Startup class - see also the occurrence-cli project.
 */
public class OccurrenceProcessorModule extends PrivateServiceModule {

  private static final String PREFIX = "occurrencestore.processor.";

  public OccurrenceProcessorModule(Properties properties) {
    super(PREFIX, properties);
  }

  @Override
  protected void configureService() {
    install(new OccurrencePersistenceModule(getVerbatimProperties()));
    expose(OccurrenceService.class);
    expose(OccurrencePersistenceService.class);
    expose(OccurrenceKeyPersistenceService.class);
    expose(VerbatimOccurrencePersistenceService.class);
    expose(FragmentPersistenceService.class);
    install(new PostalServiceModule("occurrencestore", getVerbatimProperties()));
    expose(MessagePublisher.class);
    expose(ZookeeperConnector.class);
  }

  @Provides
  public ZookeeperConnector provideZookeeperConnector(@Named("zookeeper.connection_string") String zkUrl)
    throws Exception {
    CuratorFramework curator =
      CuratorFrameworkFactory.builder().namespace("crawler").connectString(zkUrl).retryPolicy(new RetryNTimes(1, 1000))
        .build();
    curator.start();
    return new ZookeeperConnector(curator);
  }
}