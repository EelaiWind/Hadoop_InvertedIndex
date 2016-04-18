package invertedIndex;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;


public class FileSectionReducer extends Reducer<WordFileIdPositionPair,FileSection,Text,Text> {
	private Text outputKey = new Text();
	private Text outputValue = new Text();

	public void reduce(WordFileIdPositionPair key, Iterable<FileSection> values, Context context) throws IOException, InterruptedException {
		FileSection fileSectionCollection = new FileSection();
		outputKey.set(key.getWord());
		fileSectionCollection.setFileId(key.getFileId());
		fileSectionCollection.clearPositions();
		for ( FileSection filesection : values ){
			fileSectionCollection.addUniquePositions(filesection.getPositions());
		}
		outputValue.set(fileSectionCollection.toString());
		context.write(outputKey, outputValue);
	}
}
