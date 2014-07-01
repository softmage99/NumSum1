package reduce;

import java.io.IOException;
import java.util.Iterator;

import mainClass.AgmVO;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reduce extends MapReduceBase implements Reducer<Text, AgmVO, Text, AgmVO>{

	private AgmVO agmVO = new AgmVO();
	
	@Override
	public void reduce(Text key, Iterator<AgmVO> values,
			OutputCollector<Text, AgmVO> output, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		int sum = 0;
		int i=0;
		
		 while (values.hasNext()){
			 AgmVO tempAgmVO = (AgmVO)values.next(); 
			if(i==0){
				agmVO.setMaxDif(tempAgmVO.getMinDif());
				agmVO.setMinDif(tempAgmVO.getMaxDif());
				agmVO.setCtr(tempAgmVO.getCtr());
				i+=1;
			}else {
				if(agmVO.getMinDif()==0 || 
						tempAgmVO.getMinDif()<agmVO.getMinDif()){
					
					agmVO.setMinDif(tempAgmVO.getMinDif());
					
				}
				
				if(agmVO.getMaxDif()==0 || 
						tempAgmVO.getMaxDif()>agmVO.getMaxDif()){
					
					agmVO.setMaxDif(tempAgmVO.getMaxDif());
					
				}
				
			}
			sum += tempAgmVO.getCtr();
			
		}
		
		agmVO.setCtr(sum);
		
		output.collect(key, agmVO);
	
	}

}
