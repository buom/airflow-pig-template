S = LOAD '${inputPath}' USING PigStorage('\t');

R = FOREACH S GENERATE ...

--STORE R INTO '${table}' USING org.apache.hive.hcatalog.pig.HCatStorer('ds=${datestamp}');
STORE R INTO '${outputPath}' USING PigStorage('\t','-schema');
