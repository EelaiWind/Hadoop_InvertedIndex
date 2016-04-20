package invertedIndex;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;


public class FileSectionReducer extends Reducer<WordFileIdPositionPair,LongWritable,Text,Text> {
	private Text outputKey = new Text();
	private Text outputValue = new Text();

	public void reduce(WordFileIdPositionPair key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		String positionCollection = "";
		outputKey.set(key.getWord());
		int termFrequency = 0;

		for ( LongWritable position : values ){
			termFrequency += 1;
			positionCollection += " " + position;
		}
		positionCollection = key.getFileId() + " " + termFrequency + positionCollection;
		outputValue.set(positionCollection);
		context.write(outputKey, outputValue);
	}
}
