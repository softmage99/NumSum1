package mainClass;

import java.io.IOException;

import map.Map;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import reduce.Reduce;
import reduce.Reduce1;


public class AgmMinMaxCtr {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		JobConf conf = new JobConf(AgmMinMaxCtr.class);
		 conf.setJobName("AgmMinMaxCtr");
		 
		 conf.setOutputKeyClass(Text.class);
		 conf.setOutputValueClass(AgmVO.class);
		 
		conf.setMapperClass(Map.class);
		// conf.setMapOutputKeyClass(Map1.class);
		 //conf.setMapOutputValueClass(Map1.class);
		 
		 conf.setCombinerClass(Reduce.class);
		 conf.setReducerClass(Reduce.class);
		 //conf.setCombinerClass(Reduce1.class);
		 //conf.setReducerClass(Reduce1.class);
		 
		 conf.setInputFormat(TextInputFormat.class);
		 conf.setOutputFormat(TextOutputFormat.class);
		 
		 FileInputFormat.setInputPaths(conf, new Path(args[0]));
		 FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		 
		 JobClient.runJob(conf);
	}

}
