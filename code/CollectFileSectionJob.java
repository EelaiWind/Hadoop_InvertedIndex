package invertedIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.net.URI;
import org.apache.hadoop.fs.Path;

public class CollectFileSectionJob {

	public static int main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "CollectFileSectionJob");
		job.setJarByClass(CollectFileSectionJob.class);

		// set the class of each stage in mapreduce
		job.setMapperClass(CollectFileSectionMapper.class);
		job.setReducerClass(CollectFileSectionReducer.class);

		job.setGroupingComparatorClass(WordFileIdGroupComparator.class);
		//job.setPartitionerClass(WordFilePartitioner.class);

		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(WordFileIdPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// set the number of reducer
		job.setNumReduceTasks(1);

		// add input/output path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
