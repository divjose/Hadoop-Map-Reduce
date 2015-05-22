
//Divya Maira  Jose
//CSE - 6331
//Programming Assignment 2


import java.io.IOException;
import java.util.*;  	
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class WordLineCount{
 	
 	   public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntWritable> {
 	     private final static IntWritable one = new IntWritable(1);
 	     private Text word = new Text();
 	
 	     public void map(LongWritable key, Text value, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
 	       String line = value.toString();
 	       StringTokenizer tokenizer = new StringTokenizer(line);
 	       while (tokenizer.hasMoreTokens()) {
                     String token = tokenizer.nextToken();
		     Integer len=new Integer(token.length());
                     
		    if(token.matches("^[a-zA-Z]+$")){                                    
		     word.set(token);   
                     
			if(len<=6)
                        {
                        output.collect(new IntWritable(len),new IntWritable(1));
                        }
                        else
                        {
                         len=7;
                         output.collect(new IntWritable(len),new IntWritable(1));                           
                        }
                                                 		 
                 //output.collect(new IntWritable(token.length()),new IntWritable(1));
                 }                                 		               

 	       }
 	     }
 	   }

public static class Reduce extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	     public void reduce(IntWritable key, Iterator<IntWritable> values, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
 	       int sum = 0;
 	       while (values.hasNext()) {
 	        sum += values.next().get();
                    
 	       }
 	       output.collect(key, new IntWritable(sum));
 	     }
 	   }

public static void main(String[] args) throws Exception {
	     JobConf conf = new JobConf(WordLineCount.class);
 	     conf.setJobName("wordlength");
 	
 	     conf.setOutputKeyClass(IntWritable.class);
 	     conf.setOutputValueClass(IntWritable.class);
 	
             conf.setNumMapTasks(10);
             conf.setNumReduceTasks(1);
             

	     conf.setMapperClass(Map.class);
 	     conf.setCombinerClass(Reduce.class);
 	     conf.setReducerClass(Reduce.class);
 	
 	     conf.setInputFormat(TextInputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);
 	
 	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
 	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));
 	
 	     JobClient.runJob(conf);
 	   }
 	}
 	
