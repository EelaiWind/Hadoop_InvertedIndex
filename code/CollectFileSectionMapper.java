package invertedIndex;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class CollectFileSectionMapper extends Mapper<LongWritable, Text, WordFileIdPair, FileSectionCollection>{
	private WordFileIdPair outputkey = new WordFileIdPair();
	private FileSectionCollection outputValue = new FileSectionCollection();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split("\\s",3);
		outputkey.setWord(tokens[0]);
		outputkey.setFileId(Integer.parseInt(tokens[1]));
		outputValue.setDocumentFrequency(1);
		outputValue.setFileSections(tokens[1] + " " + tokens[2]);
		context.write(outputkey, outputValue);
	}
}