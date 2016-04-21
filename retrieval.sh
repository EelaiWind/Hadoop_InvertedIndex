# Do not uncomment these lines to directly execute the script
# Modify the path to fit your need before using this script
#hdfs dfs -rm -r /user/TA/WordCount/Output/
#hadoop jar WordCount.jar wordcount.WordCount /user/shared/WordCount/Input /user/TA/WordCount/Output
#hdfs dfs -cat /user/TA/WordCount/Output/part-*

HOME=/user/s101062105/invertedIndex/
INPUT=${HOME}output
OUTPUT=${HOME}retrieval_result
DOCUMENT=${HOME}input

hdfs dfs -rm -r $OUTPUT
hadoop jar InvertedIndex.jar invertedIndex.RetrievalJob $INPUT $OUTPUT $DOCUMENT $*
hadoop fs -cat ${OUTPUT}/* > retrievalOutput.log

