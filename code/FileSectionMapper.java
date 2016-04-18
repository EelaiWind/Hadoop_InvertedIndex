package invertedIndex;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FileSectionMapper extends Mapper<LongWritable, Text, WordFileIdPositionPair, FileSection> {
	private FileNameAndIdConvertor convertor;
	private WordFileIdPositionPair outputKey = new WordFileIdPositionPair();
	private FileSection outputValue = new FileSection();
	private Pattern wordPattern;

	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		convertor = new FileNameAndIdConvertor(context);
		wordPattern = Pattern.compile("[a-zA-Z]+");
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Matcher matcher = wordPattern.matcher(value.toString());
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		int fileId = convertor.getFileId(fileName);
		int position = (int)key.get();

		System.out.print("LOG " + key.get());
		while ( matcher.find() ){
			String word = matcher.group();
			outputKey.set(word, fileId, position);
			outputValue.setFileId(fileId);
			outputValue.clearPositions();
			outputValue.addUniquePosition(position);
			context.write(outputKey, outputValue);
			System.out.print(" " + word);
		}
		System.out.println();
	}

}
