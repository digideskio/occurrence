#!/usr/bin/env bash
curl -XDELETE 'http://uatsolr-vh.gbif.org:9200/occurrence/'
curl -XPOST 'http://uatsolr-vh.gbif.org:9200/occurrence/' -d @type_mappings.json

curl -XGET 'http://uatsolr-vh.gbif.org:9200/occurrence/occurrence/_search?q=country:us' -d '{
    "size": 0,
    "aggs" : {
        "grades_count" : { "terms" : { "field" : "basis_of_record" } }
    }
}'                                                                                                                       l
