package invertedIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;

import java.io.OutputStreamWriter;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;

public class FileIdRecorder{
	private Configuration conf;

	public FileIdRecorder(Configuration conf){
		this.conf = conf;
	}
	public void writeFileIDs(String filesDirectory) throws IOException{
		FileSystem outputFileSystem = FileSystem.get(conf);
		FileSystem inputFileSystem = FileSystem.get(conf);

		BufferedWriter writer = new BufferedWriter(
			new OutputStreamWriter(
				outputFileSystem.create( new Path(InvertedIndexSetting.ID_FILE_PATH), true ) ,"UTF-8" )
		);
		int id = 0;
		for (FileStatus status : inputFileSystem.listStatus(new Path(filesDirectory))) {
			writer.write(id+" "+ status.getPath().getName()+"\n");
			id += 1;
		}
		writer.close();
		outputFileSystem.close();
		inputFileSystem.close();
	}
}