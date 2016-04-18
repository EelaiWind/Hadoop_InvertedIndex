# Do not uncomment these lines to directly execute the script
# Modify the path to fit your need before using this script
#hdfs dfs -rm -r /user/TA/WordCount/Output/
#hadoop jar WordCount.jar wordcount.WordCount /user/shared/WordCount/Input /user/TA/WordCount/Output
#hdfs dfs -cat /user/TA/WordCount/Output/part-*

HOME=/user/s101062105/invertedIndex/
INPUT=${HOME}input
TMP_OUTPUT=${HOME}tmp_output
OUTPUT=${HOME}output

hdfs dfs -rm -r $TMP_OUTPUT
hadoop jar InvertedIndex.jar invertedIndex.FileSectionJob $INPUT $TMP_OUTPUT
hdfs dfs -rm -r $OUTPUT
hadoop jar InvertedIndex.jar invertedIndex.CollectFileSectionJob $TMP_OUTPUT $OUTPUT
hadoop fs -cat ${OUTPUT}/* > output.log

