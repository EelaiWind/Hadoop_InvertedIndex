package invertedIndex;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;


public class CollectFileSectionReducer extends Reducer<WordFileIdPair,FileSectionCollection,Text,Text> {
	private Text outputKey = new Text();
	private Text outputValue = new Text();

	public void reduce(WordFileIdPair key, Iterable<FileSectionCollection> values, Context context) throws IOException, InterruptedException {
		String result = "";
		int count = 0;
		boolean isFirst = true;

		for (FileSectionCollection value : values){
			count += value.getDocumentFrequency();
			if ( isFirst ){
				isFirst = false;
				result = value.getFileSections();
			}
			else{
				result += " " + value.getFileSections();
			}

		}
		outputKey.set(key.getWord());
		outputValue.set(count + " " + result);
		context.write(outputKey, outputValue);
	}
}
