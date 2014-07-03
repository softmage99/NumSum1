package map;

import java.io.IOException;

import mainClass.AgmVO;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class Map1 extends Mapper<Object, Text, Text, AgmVO>{
	
	private Text outUserId = new Text();
	private AgmVO agmVO = new AgmVO();
	private final static Integer one = new Integer(1);
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		if(value!=null){
			String line = value.toString();
			String data[] = line.split(",");
			if(data[0]!=null && data.length==9){
				outUserId.set(data[0]);
				if(data[6]!=null && data[7]!=null && !data[6].equals("MIN Price")){
					//Integer val = Integer.parseInt(data[7])-Integer.parseInt(data[6]);
					Double val = Double.parseDouble(data[7])-Double.parseDouble(data[6]);
					agmVO.setMinDif(val);
					agmVO.setMaxDif(val);
					agmVO.setCtr(one);
					context.write(outUserId, agmVO);
				}
			}
		}
	}

}
