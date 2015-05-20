package org.gbif.occurrence.download.file.common;

import org.gbif.common.search.util.SolrConstants;
import org.gbif.occurrence.download.file.DownloadFileWork;
import org.gbif.occurrence.search.solr.OccurrenceSolrField;

import java.util.Iterator;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;

/**
 *  Executes a Solr query and applies a predicate to each result.
 */
public class SolrQueryProcessor {

  // Default page size for Solr queries.
  private static final int LIMIT = 300;


  /**
   * Executes a query on the SolrServer parameter and applies the predicate to each result.
   * @param downloadFileWork  it's used to determine how to page through the results and the Solr query to be used
   * @param solrServer that executes the query
   * @param resultHandler predicate that process each result, receives as parameter the occurrence key
   *
   */
  public static void processQuery(final DownloadFileWork downloadFileWork, final Predicate<Integer> resultHandler) {

    // Calculates the amount of output records
    int nrOfOutputRecords = downloadFileWork.getTo() - downloadFileWork.getFrom();

    // Creates a search request instance using the search request that comes in the fileJob
    SolrQuery solrQuery = createSolrQuery(downloadFileWork.getQuery());

    try {
      int recordCount = 0;
      while (recordCount < nrOfOutputRecords) {
        solrQuery.setStart(downloadFileWork.getFrom() + recordCount);
        // Limit can't be greater than the maximum number of records assigned to this job
        solrQuery.setRows(recordCount + LIMIT > nrOfOutputRecords ? nrOfOutputRecords - recordCount : LIMIT);
        final QueryResponse response = downloadFileWork.getSolrServer().query(solrQuery);
        for (Iterator<SolrDocument> itResults = response.getResults().iterator(); itResults.hasNext(); recordCount++) {
          resultHandler.apply((Integer) itResults.next().getFieldValue(OccurrenceSolrField.KEY.getFieldName()));
        }
      }
    } catch (SolrServerException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Creates a SolrQuery that contains the query parameter as the filter query value.
   */
  private static SolrQuery createSolrQuery(String query) {
    SolrQuery solrQuery = new SolrQuery();
    solrQuery.setQuery(SolrConstants.DEFAULT_QUERY);
    if (!Strings.isNullOrEmpty(query)) {
      solrQuery.addFilterQuery(query);
    }
    return solrQuery;
  }

  /**
   * Hidden constructor.
   */
  private SolrQueryProcessor() {
    //empty constructor
  }
}
