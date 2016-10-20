import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;



public class work1_2 {
  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text,IntWritable, Text>{

	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, Text> output, Reporter arg3)
			throws IOException {
		String str=value.toString();
		String [] arr=str.split("-");
		String year =arr[0];
		output.collect(new IntWritable
				(Integer.parseInt(year)),value);
	}
  
  }
  public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>{

	public void reduce(IntWritable key, Iterator<Text> values,
			OutputCollector<IntWritable,  Text> output, Reporter arg3) throws IOException {
                List<String> arr=new ArrayList<String>();
                 for(;values.hasNext();){
                	 Text value =values.next();
                	 arr.add(value.toString());
                	 
                 } Comparator<String> comparator = new Comparator<String>(){

					public int compare(String o1, String o2) {
						String str[]=o1.split("\t");
						String str1[]=o2.split("\t");
						if(Integer.parseInt(str[1])==Integer.parseInt(str1[1])){return 0;}
						return Integer.parseInt(str[1])<Integer.parseInt(str1[1])?-1:1;
					}           		    
                		  };  
                 Collections.sort(arr,comparator);
                 for(String a:arr){
                	 output.collect(key, new Text(a));
                 }
                 }

	  
  }
  public  static void main(String [] args) throws IOException{
	 
	  JobConf conf = new JobConf( work1_1.class);
		conf.setJobName("lalala");
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
  }
}
