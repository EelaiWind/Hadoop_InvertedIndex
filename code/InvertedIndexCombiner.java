package invertedIndex;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;


public class InvertedIndexCombiner extends Reducer<WordFileIdPositionPair,FileSection,WordFileIdPositionPair,FileSection> {
	private WordFileIdPositionPair outputKey = new WordFileIdPositionPair();
	private FileSection outputValue = new FileSection();

	public void reduce(WordFileIdPositionPair key, Iterable<FileSection> values, Context context) throws IOException, InterruptedException {
		outputValue.setFileId(key.getFileId());
		outputValue.clearPositions();
		for ( FileSection filesection : values ){
			outputValue.addUniquePositions(filesection.getPositions());
		}
		context.write(key, outputValue);
	}
}
