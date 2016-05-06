package org.gbif.occurrence.es.index.mr;

import org.gbif.occurrence.avro.model.Occurrence;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.base.Strings;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OccurrenceAvroMapper  extends MapReduceBase implements
  Mapper<AvroWrapper<Occurrence>, NullWritable, NullWritable,MapWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceAvroMapper.class);

  @Override
  public void map(AvroWrapper<Occurrence> occurrenceAvro, NullWritable value, OutputCollector<NullWritable,MapWritable> collector,
                  Reporter reporter) throws IOException {
    // create the MapWritable object
    MapWritable doc = new MapWritable();
    Occurrence occurrence = occurrenceAvro.datum();
    doc.put(new Text("key"), new IntWritable(occurrence.getKey()));
    putIfNotNull(doc,"dataset_key",occurrence.getDatasetKey());
    putIfNotNull(doc,"institution_code",occurrence.getInstitutionCode());

    putIfNotNull(doc,"collection_code",occurrence.getCollectionCode());
    putIfNotNull(doc,"catalog_number",occurrence.getCatalogNumber());

    putIfNotNull(doc,"recorded_by",occurrence.getRecordedBy());
    putIfNotNull(doc,"record_number",occurrence.getRecordNumber());
    putIfNotNull(doc,"last_interpreted",occurrence.getLastInterpreted());

    putIntegerArrayWritable(doc,"taxon_key",occurrence.getTaxonKey());

    putIntIfNotNull(doc,"kingdom_key",occurrence.getKingdomKey());
    putIntIfNotNull(doc,"phylum_key",occurrence.getPhylumKey());
    putIntIfNotNull(doc,"class_key",occurrence.getClassKey());
    putIntIfNotNull(doc,"order_key",occurrence.getOrderKey());
    putIntIfNotNull(doc,"family_key",occurrence.getFamilyKey());
    putIntIfNotNull(doc,"genus_key",occurrence.getGenusKey());
    putIntIfNotNull(doc,"subgenus_key",occurrence.getSubgenusKey());
    putIntIfNotNull(doc,"species_key",occurrence.getSpeciesKey());
    putIfNotNull(doc,"country",occurrence.getCountry());
    putIfNotNull(doc,"continent",occurrence.getContinent());
    putIfNotNull(doc,"publishing_country",occurrence.getPublishingCountry());


    putDoubleIfNotNull(doc,"latitude",occurrence.getLatitude());
    putDoubleIfNotNull(doc,"longitude",occurrence.getLongitude());
    if (occurrence.getLatitude() != null && occurrence.getLongitude() != null) {
      doc.put(new Text("coordinate"), new Text(occurrence.getLatitude().toString() + ',' + occurrence.getLongitude().toString()));
    }

    putIntIfNotNull(doc,"year",occurrence.getYear());
    putIntIfNotNull(doc,"month",occurrence.getMonth());
    putIfNotNull(doc,"event_date",occurrence.getEventDate());
    putIfNotNull(doc,"basis_of_record",occurrence.getBasisOfRecord());
    putIfNotNull(doc,"type_status",occurrence.getTypeStatus());

    putBooleanIfNotNull(doc,"spatial_issues",occurrence.getSpatialIssues());
    putBooleanIfNotNull(doc,"has_coordinate",occurrence.getHasCoordinate());

    putIntIfNotNull(doc,"elevation",occurrence.getElevation());
    putIntIfNotNull(doc,"depth",occurrence.getDepth());
    putIfNotNull(doc,"establishment_means",occurrence.getEstablishmentMeans());
    putIfNotNull(doc,"occurrence_id",occurrence.getOccurrenceId());

    //putStringArrayWritable(doc, "media_type", occurrence.getMediaType());
    putStringArrayWritable(doc, "issue", occurrence.getIssue());

    putIfNotNull(doc,"scientific_name",occurrence.getScientificName());
    // write the result to the output collector
    // one can pass whatever value to the key; EsOutputFormat ignores it
    LOG.info("Object: " + doc.toString());
    for(Map.Entry<Writable,Writable> entry : doc.entrySet()) {
     System.out.println(entry.getKey().toString() + ":" + entry.getValue().toString());
    }
    collector.collect(NullWritable.get(),doc);
  }

  private static <T> void putIfNotNull(MapWritable doc, String field, String value) {
    if (!Strings.isNullOrEmpty(value)) {
      doc.put(new Text(field), new Text(value));
    }
  }

  private static <T> void putIntIfNotNull(MapWritable doc, String field, Integer value) {
    if (value != null) {
      doc.put(new Text(field), new IntWritable(value));
    }
  }

  private static <T> void putDoubleIfNotNull(MapWritable doc, String field, Double value) {
    if (value != null) {
      doc.put(new Text(field), new DoubleWritable(value));
    }
  }

  private static <T> void putBooleanIfNotNull(MapWritable doc, String field, Boolean value) {
    if (value != null) {
      doc.put(new Text(field), new BooleanWritable(value));
    }
  }

  private static void putStringArrayWritable(MapWritable doc, String field, List<String> values) {
    if(values != null && !values.isEmpty()) {
      Text[] writables = new Text[values.size()];
      for(int i = 0; i < values.size(); i++ ){
        writables[i] = new Text(values.get(i));
      }
      doc.put(new Text(field), new StringArrayWritable(writables));
    }
  }

  private static void putIntegerArrayWritable(MapWritable doc, String field, List<Integer> values) {
    if(values != null && !values.isEmpty()) {
      IntWritable[] writables = new IntWritable[values.size()];
      for(int i = 0; i < values.size(); i++ ){
        writables[i] = new IntWritable(values.get(i));
      }
      doc.put(new Text(field), new IntegerArrayWritable(writables));
    }
  }

}
