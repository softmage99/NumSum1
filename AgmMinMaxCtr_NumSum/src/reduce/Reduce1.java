package reduce;

import java.io.IOException;

import mainClass.AgmVO;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class Reduce1 extends
Reducer<Text, AgmVO, Text, AgmVO>{

	private AgmVO agmVO = new AgmVO();
	
	public void reduce(Text key, Iterable<AgmVO> values,
			Context context) throws IOException, InterruptedException {
		
		
		int sum = 0;
		int i=0;
		
		for(AgmVO agmVO_val : values){
			
			if(i==0){
				agmVO.setMaxDif(agmVO_val.getMinDif());
				agmVO.setMinDif(agmVO_val.getMaxDif());
				agmVO.setCtr(agmVO_val.getCtr());
				i+=1;
			}else {
				if(agmVO.getMinDif()==0 || 
						agmVO_val.getMinDif()<agmVO.getMinDif()){
					
					agmVO.setMinDif(agmVO_val.getMinDif());
					
				}
				
				if(agmVO.getMaxDif()==0 || 
						agmVO_val.getMaxDif()>agmVO.getMaxDif()){
					
					agmVO.setMaxDif(agmVO_val.getMaxDif());
					
				}
				
			}
			sum += agmVO_val.getCtr();
			
		}
		
		agmVO.setCtr(sum);
		
		context.write(key, agmVO);
	}
}
