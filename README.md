# csvfix

Pre-processes CSV file on HDFS that have newline within quoted fields.

While it is valid to have newlines within quoted fields in CSV files, HDFS splits blocks based on \n regardless of where it is.

This is a problem for packages such as spark-csv that cannot handle newlines other than line terminators, irrespective of whether it is valid CSV.

This map-reduce module reads files on HDFS that contain newline characters and escapes them so the file can be read by other packages.

This program depends on https://github.com/markmo/CSVInputFormat.

There may be risk of loss of a few records that cross the split boundary. This area needs further testing.

For large scale analytics applications, this risk may be acceptable.

Best to include the escaping rules in the ingestion process to begin with.

## Usage

    hadoop jar lib/csvfix-assembly-1.0-SNAPSHOT.jar App <isZip: 0|1> <outputCompression: snappy|gzip> <inputPath> <outputPath> <startDate e.g. "2016-02-01"> <endDate (exclusive)>
