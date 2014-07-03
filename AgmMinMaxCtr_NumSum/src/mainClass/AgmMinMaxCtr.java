package mainClass;

import java.io.IOException;

/*import map.Map;*/
import map.Map1;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
/*import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;*/
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*import reduce.Reduce;*/
import reduce.Reduce1;


public class AgmMinMaxCtr {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		 Configuration conf = new Configuration();
		 
		 @SuppressWarnings("deprecation")
		Job job = new Job(conf, "AgmMinMaxCtr");
		 job.setJarByClass(AgmMinMaxCtr.class);
		 
		 job.setMapperClass(Map1.class);
		 job.setReducerClass(Reduce1.class);

		 job.setInputFormatClass(TextInputFormat.class);
		 TextInputFormat.setInputPaths(job, args[0]);
		 
		 job.setOutputFormatClass(TextOutputFormat.class);
		 TextOutputFormat.setOutputPath(job, new Path(args[1]));
		 
		 job.setMapOutputKeyClass(Text.class);
		 job.setMapOutputValueClass(AgmVO.class);
		 
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(AgmVO.class);
		 
		// conf.setMapOutputKeyClass(Map1.class);
		// conf.setMapOutputValueClass(Map1.class);
		 
		// conf.setCombinerClass(Reduce.class);
		 //conf.setReducerClass(Reduce.class);
		 //conf.setCombinerClass(Reduce1.class);
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
