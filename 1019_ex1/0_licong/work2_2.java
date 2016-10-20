package work2;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
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


public class work2_2 {
	 public static class Map extends MapReduceBase implements Mapper<LongWritable, Text,Text,Text>{

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			               String line = value.toString();
			               String str1[]= line.split(" ",2);
			               String user =str1[0];
			               String friends =str1[1];
			               String str2[]=str1[1].split(" ");
			              for(String friend : str2) {        
			                int result = friend.compareTo(user);
			                   if(result > 0) {
			                   output.collect(new Text(user + friend), new Text(friends));
			                  } 
			              else if(result < 0) {
			            	  output.collect(new Text(friend+user), new Text(friends));
			                  }
			              }
			        }
	  
		  }
		  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

			public void reduce(Text key, Iterator<Text> values,
					OutputCollector<Text,  Text> output, Reporter arg3) throws IOException {
				ArrayList<String> list=new ArrayList<String>();
				 String d ="   ";
				 String c="["+key+"]"+" ";
				while (values.hasNext()){
				String a=values.next().toString();
				list.add(a);
				}
			    if(list.size()==2){
				 String str[]=list.get(0).split(" ");
				 String str1[]=list.get(1).split(" ");
				
				 for(String a: str){
					for(String b:str1){
						
						if(a.equals(b)){
							d=d+","+a;
							}
					}
				}
			    }
				 
			 output.collect( new Text(c), new Text(d));
			}

		  }  
		  
		  public  static void main(String [] args) throws IOException{
			 
			  JobConf conf = new JobConf(work2_2.class);
				conf.setJobName("lalala");
				conf.setInputFormat(TextInputFormat.class);
				conf.setOutputFormat(TextOutputFormat.class);
				conf.setOutputKeyClass(Text.class);
				conf.setOutputValueClass(Text.class);
				conf.setMapperClass(Map.class);
				conf.setReducerClass(Reduce.class);
				FileInputFormat.setInputPaths(conf, new Path(args[0]));
				FileOutputFormat.setOutputPath(conf, new Path(args[1]));
				JobClient.runJob(conf);
		  }
}
