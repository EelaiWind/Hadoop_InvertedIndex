package invertedIndex;

import org.apache.hadoop.mapreduce.Partitioner;

public class WordFilePartitioner extends Partitioner<WordFileIdPositionPair, FileSection>{

	@Override
	public int getPartition(WordFileIdPositionPair key, FileSection value, int numPartitions){
		if ( key.hashcode() >= 0){
			return  key.hashcode() % numPartitions;
		}
		else{
			return (-1*key.hashcode()) % numPartitions;
		}
	}
}