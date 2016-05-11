#!/usr/bin/env bash
curl -XDELETE 'http://uatsolr-vh.gbif.org:9200/occurrence/'
curl -XPOST 'http://uatsolr-vh.gbif.org:9200/occurrence/' -d @type_mappings.json
