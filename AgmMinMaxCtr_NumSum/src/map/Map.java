package map;

import java.io.IOException;

import mainClass.AgmVO;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Map extends MapReduceBase implements
Mapper<LongWritable, Text, Text, AgmVO> {

	private Text outUserId = new Text();
	private AgmVO agmVO = new AgmVO();
	private final static int one = 1;
	
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, AgmVO> output, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		if(value!=null){
			String line = value.toString();
			String data[] = line.split(",");
			if(data[0]!=null && data.length==9){
				outUserId.set(data[0]);
				if(data[6]!=null && data[7]!=null && !data[6].equals("MIN Price")){
					Double val = Double.parseDouble(data[7])-Double.parseDouble(data[6]);
					agmVO.setMinDif(val);
					agmVO.setMaxDif(val);
					agmVO.setCtr(one);
					output.collect(outUserId, agmVO);
				}
			}
		}
	}

}
