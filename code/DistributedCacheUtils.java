package invertedIndex;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

class DistributedCacheUtils{
	static HashMap<String, Integer> loadFileId(JobContext context) throws IOException{
		HashMap<String, Integer> fileNameToId = new HashMap<String, Integer>();

		FileSystem fileSystem = FileSystem.get(context.getConfiguration());
		URI[] cacheFiles = context.getCacheFiles();
		Path idFilePath = new Path(cacheFiles[0].getPath());
		BufferedReader reader = new BufferedReader(
			new InputStreamReader(fileSystem.open(idFilePath))
		);

		String line;
		while((line = reader.readLine() ) != null){
			String[] parameter = line.split(" ", 2);
			if (parameter.length < 2 ){
				continue;
			}
			fileNameToId.put(parameter[1], Integer.parseInt(parameter[0]));
		}

		reader.close();
		return fileNameToId;
	}
}