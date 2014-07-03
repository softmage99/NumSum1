package mainClass;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class AgmVO implements Writable{

	private Double maxDif=0.0d;
	private Double minDif=0.0d;
	private Integer ctr= new Integer(0);
	
	
	
	public String toString(){
		return "min = "+minDif+" max = "+ maxDif+" ctr = "+ctr;
		
	}
	@Override
	public void readFields(DataInput inp) throws IOException {
		// TODO Auto-generated method stub
		minDif = inp.readDouble();
		maxDif = inp.readDouble();
		ctr = inp.readInt();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeDouble(getMaxDif());
		out.writeDouble(getMinDif());
		out.write(getCtr());
	}
	public Double getMaxDif() {
		return maxDif;
	}
	public void setMaxDif(Double maxDif) {
		this.maxDif = maxDif;
	}
	public Double getMinDif() {
		return minDif;
	}
	public void setMinDif(Double minDif) {
		this.minDif = minDif;
	}
	public Integer getCtr() {
		return ctr;
	}
	public void setCtr(Integer ctr) {
		this.ctr = ctr;
	}
	
}
