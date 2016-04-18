package invertedIndex;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

public class FileSectionCollection implements Writable{
	private int documentFrequency;
	private Text fileSections;

	public FileSectionCollection(){
		documentFrequency = 0;
		fileSections = new Text();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(documentFrequency);
		fileSections.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		documentFrequency = in.readInt();
		fileSections.readFields(in);
	}

	public void setDocumentFrequency(int documentFrequency){
		this.documentFrequency = documentFrequency;
	}

	public void setFileSections(String fileSections){
		this.fileSections.set(fileSections);
	}

	public int getDocumentFrequency(){
		return documentFrequency;
	}

	public String getFileSections(){
		return fileSections.toString();
	}
}