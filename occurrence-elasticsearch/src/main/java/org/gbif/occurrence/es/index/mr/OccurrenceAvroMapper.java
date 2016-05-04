package org.gbif.occurrence.es.index.mr;

import org.gbif.occurrence.avro.model.Occurrence;

import java.io.IOException;
import java.util.List;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.ArrayWritable;
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

public class OccurrenceAvroMapper  extends MapReduceBase implements Mapper<AvroKey<Occurrence>, NullWritable, NullWritable, MapWritable> {

  @Override
  public void map(AvroKey<Occurrence> key, NullWritable value, OutputCollector<NullWritable,MapWritable> output,
                  Reporter reporter) throws IOException {
    // create the MapWritable object
    MapWritable doc = new MapWritable();

    Occurrence occurrence = key.datum();

    doc.put(new Text("key"), new IntWritable(occurrence.getKey()));
    doc.put(new Text("dataset_key"), new Text(occurrence.getDatasetKey()));
    doc.put(new Text("institution_code"), new Text(occurrence.getInstitutionCode()));
    doc.put(new Text("collection_code"), new Text(occurrence.getCollectionCode()));
    doc.put(new Text("catalog_number"), new Text(occurrence.getCatalogNumber()));
    doc.put(new Text("recorded_by"), new Text(occurrence.getRecordedBy()));
    doc.put(new Text("record_number"), new Text(occurrence.getRecordNumber()));
    doc.put(new Text("last_interpreted"), new Text(occurrence.getLastInterpreted()));

    if(occurrence.getTaxonKey() != null && !occurrence.getTaxonKey().isEmpty()) {
      ArrayPrimitiveWritable taxonKeys = new ArrayPrimitiveWritable(int.class);
      taxonKeys.set(occurrence.getTaxonKey().toArray(new Integer[0]));
      doc.put(new Text("taxon_key"), taxonKeys);
    }

    doc.put(new Text("kingdom_key"), new IntWritable(occurrence.getKingdomKey()));
    doc.put(new Text("phylum_key"), new IntWritable(occurrence.getPhylumKey()));
    doc.put(new Text("class_key"), new IntWritable(occurrence.getClassKey()));
    doc.put(new Text("order_key"), new IntWritable(occurrence.getOrderKey()));
    doc.put(new Text("family_key"), new IntWritable(occurrence.getFamilyKey()));
    doc.put(new Text("genus_key"), new IntWritable(occurrence.getGenusKey()));
    doc.put(new Text("subgenus_key"), new IntWritable(occurrence.getSubgenusKey()));
    doc.put(new Text("species_key"), new IntWritable(occurrence.getSpeciesKey()));
    doc.put(new Text("country"), new Text(occurrence.getCountry()));
    doc.put(new Text("continent"), new Text(occurrence.getContinent()));
    doc.put(new Text("publishing_country"), new Text(occurrence.getPublishingCountry()));
    doc.put(new Text("latitude"), new DoubleWritable(occurrence.getLatitude()));
    doc.put(new Text("longitude"), new DoubleWritable(occurrence.getLongitude()));
    if (occurrence.getLatitude() != null && occurrence.getLongitude() != null) {
      doc.put(new Text("coordinate"), new Text(occurrence.getLatitude().toString() + ',' + occurrence.getLongitude().toString()));
    }
    doc.put(new Text("year"), new IntWritable(occurrence.getYear()));
    doc.put(new Text("month"), new IntWritable(occurrence.getMonth()));
    doc.put(new Text("event_date"), new Text(occurrence.getEventDate()));
    doc.put(new Text("basis_of_record"), new Text(occurrence.getBasisOfRecord()));
    doc.put(new Text("type_status"), new Text(occurrence.getTypeStatus()));
    doc.put(new Text("spatial_issues"), new BooleanWritable(occurrence.getSpatialIssues()));
    doc.put(new Text("has_coordinate"), new BooleanWritable(occurrence.getHasCoordinate()));
    doc.put(new Text("elevation"), new IntWritable(occurrence.getElevation()));
    doc.put(new Text("depth"), new IntWritable(occurrence.getDepth()));
    doc.put(new Text("establishment_means"), new Text(occurrence.getEstablishmentMeans()));
    doc.put(new Text("occurrence_id"), new Text(occurrence.getOccurrenceId()));
    doc.put(new Text("media_type"), toArrayWritable(occurrence.getMediaType()));
    doc.put(new Text("issue"), toArrayWritable(occurrence.getIssue()));
    doc.put(new Text("scientific_name"), new Text(occurrence.getScientificName()));
    // write the result to the output collector
    // one can pass whatever value to the key; EsOutputFormat ignores it
    output.collect(NullWritable.get(), doc);
  }

  private Writable toArrayWritable(List<String> values) {
    if(values != null && !values.isEmpty()) {
      Text[] writables = new Text[values.size()];
      for(int i = 0; i < values.size(); i++ ){
        writables[i] = new Text(values.get(i));
      }
      return new ArrayWritable(Text.class,writables);
    }
    return NullWritable.get();
  }
}
