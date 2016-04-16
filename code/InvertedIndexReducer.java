package invertedIndex;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;


public class InvertedIndexReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	private IntWritable id = new IntWritable();
	private Text fileName = new Text();

	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		HashMap<String, Integer> fileNameToId = DistributedCacheUtils.loadFileId(context);
		for (Map.Entry<String, Integer> entry : fileNameToId.entrySet()) {
			fileName.set(entry.getKey());
			id.set(entry.getValue());
			context.write(fileName, id);
		}
	}

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

	}
}
