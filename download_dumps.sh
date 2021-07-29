#!/bin/bash

DOWNLOAD_DIR="./databases"
mkdir -p $DOWNLOAD_DIR

function download {
  name=$(echo $1 |awk -F "/" '{print $NF}')
  echo "Downloading $name..."
  wget -O "$DOWNLOAD_DIR/$name" "$1"
}

#overall asn list
download "ftp://ftp.ripe.net/ripe/asnames/asn.txt"

#delegated extended version 
download "ftp://ftp.arin.net/pub/stats/arin/delegated-arin-extended-latest"
download "ftp://ftp.ripe.net/ripe/stats/delegated-ripencc-extended-latest"
download "ftp://ftp.afrinic.net/pub/stats/afrinic/delegated-afrinic-extended-latest"
download "ftp://ftp.apnic.net/pub/stats/apnic/delegated-apnic-extended-latest"
download "ftp://ftp.lacnic.net/pub/stats/lacnic/delegated-lacnic-extended-latest" 

#main db (AFRINIC, ARIN, LACNIC)
download "https://ftp.afrinic.net/pub/dbase/afrinic.db.gz"
download "https://ftp.arin.net/pub/rr/arin.db.gz"
download "https://ftp.lacnic.net/lacnic/dbase/lacnic.db.gz"

#APNIC split by inet, inet6, asn
download "https://ftp.apnic.net/pub/apnic/whois/apnic.db.inetnum.gz"
download "https://ftp.apnic.net/pub/apnic/whois/apnic.db.inet6num.gz"
download "https://ftp.apnic.net/pub/apnic/whois/apnic.db.aut-num.gz"

# RIPE split by inet, inet6, asn
download "https://ftp.ripe.net/ripe/dbase/split/ripe.db.inetnum.gz"
download "https://ftp.ripe.net/ripe/dbase/split/ripe.db.inet6num.gz"
download "https://ftp.ripe.net/ripe/dbase/split/ripe.db.aut-num.gz"

#todo ftp://ftp.ripe.net//ripe/ipmap/geolocations-latest
