package invertedIndex;

import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

public class FileSection implements Writable {
	private int fileId;
	private ArrayList<Integer> positions;

	public FileSection(){
		this.positions = new ArrayList<Integer>();
	}

	public int getFileId(){
		return fileId;
	}

	public int getFrequency(){
		return positions.size();
	}

	public ArrayList<Integer> getPositions(){
		return positions;
	}

	public void setFileId(int fileId){
		this.fileId = fileId;
	}

	public void clearPositions(){
		positions.clear();
	}

	public void addUniquePosition(int position){
		if ( positions.indexOf(position) >= 0 ){
			return;
		}
		positions.add(position);
	}

	public void addUniquePositions(ArrayList<Integer> positions){
		for (Integer position : positions){
			addUniquePosition(position.intValue());
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(fileId);
		out.writeInt(getFrequency());
		for( Integer position : positions){
			out.writeInt(position);
		}

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		fileId = in.readInt();
		int frequency = in.readInt();
		clearPositions();
		for ( int i = 0 ; i < frequency ; i++){
			addUniquePosition(in.readInt());
		}
	}

	public String toString(){
		String result = fileId + " " + positions.size();
		for ( Integer position : positions ){
			result += " " + position;
		}
		return result;
	}
}