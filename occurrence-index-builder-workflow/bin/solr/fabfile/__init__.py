#fab install_solr_dev -u root

from fabric.api import *

env.roledefs['cluster_dev'] = ['c1n1.gbif.org','c1n2.gbif.org','c1n3.gbif.org','c2n1.gbif.org','c2n2.gbif.org','c2n3.gbif.org']

env.roledefs['cluster_prod'] = ['c4n1.gbif.org','c4n2.gbif.org','c4n3.gbif.org','c4n4.gbif.org','c4n5.gbif.org','c4n6.gbif.org','c4n7.gbif.org','c4n8.gbif.org','c4n9.gbif.org','c4n10.gbif.org','c4n11.gbif.org','c4n12.gbif.org','prodmaster1-vh.gbif.org','prodmaster2-vh.gbif.org','prodmaster3-vh.gbif.org']


@task
@parallel
@roles('cluster_dev')
def install_solr_dev():
  install_solr()


@task
@parallel
@roles('cluster_prod')
def install_solr_prod():
  install_solr()


def install_solr():
  run('''rm -rf /opt/solr-5.3.0/;\
        cd /tmp/;\
        wget http://mirrors.ucr.ac.cr/apache//lucene/solr/5.3.0/solr-5.3.0.tgz;\
        tar xzf solr-5.3.0.tgz -C /opt/;\
        rm -f solr-5.3.0.tgz;''')