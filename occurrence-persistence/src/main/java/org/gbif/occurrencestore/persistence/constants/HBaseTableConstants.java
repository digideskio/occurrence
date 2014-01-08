package org.gbif.occurrencestore.persistence.constants;

/**
 * Constants for commonly used HBase column families, column names, and row ids.
 */
public class HBaseTableConstants {

  /**
   * Should never be instantiated.
   */
  private HBaseTableConstants() {
  }

  // the one column family for all columns of the occurrence table
  public static final String OCCURRENCE_COLUMN_FAMILY = "o";

  // An occurrence can have 0-n identifiers, each of a certain type. Their column names look like t1, i1, t2, i2, etc.
  public static final String IDENTIFIER_TYPE_COLUMN = "t";
  public static final String IDENTIFIER_COLUMN = "i";

  // the counter table is a single cell that is the "autoincrement" number for new keys, with column family, column,
  // and key ("row" in hbase speak)
  public static final String COUNTER_COLUMN_FAMILY = "o";
  public static final String COUNTER_COLUMN = "id";
  public static final int COUNTER_ROW = 1;

  // the lookup table is a secondary index of unique ids (holy triplet or publisher-provided) to GBIF integer keys
  public static final String LOOKUP_COLUMN_FAMILY = "o";
  public static final String LOOKUP_KEY_COLUMN = "i";
  public static final String LOOKUP_LOCK_COLUMN = "l";
  public static final String LOOKUP_STATUS_COLUMN = "s";
}