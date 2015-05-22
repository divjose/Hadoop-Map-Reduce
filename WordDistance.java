
//Divya Maria Jose
// 1000989859
//CSE -6331 
//Programming Assignment 2 

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;



public class WordDistance {

	
	public static class ParagraphParser extends TextInputFormat{

	public RecordReader<LongWritable, Text> getRecordReader(InputSplit input, JobConf job,
			Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		return new MyRecordReader1(job, (FileSplit)input);
	}

	
	
	
}

 public static class MyRecordReader1 implements RecordReader<LongWritable, Text> 
{
	private LineRecordReader linereader;
	private LongWritable linekey;
	private Text linevalue;
	
	
	
	public MyRecordReader1(JobConf job, FileSplit split) throws IOException{
	  linereader = new LineRecordReader(job,split);
	  
	  linekey = linereader.createKey();
	  linevalue = linereader.createValue();
	  
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		linereader.close();
	}

	@Override
	public LongWritable createKey() {
		// TODO Auto-generated method stub
		return new LongWritable();
	}

	@Override
	public Text createValue() {
		// TODO Auto-generated method stub
		return new Text("");
	}

	@Override
	public long getPos() throws IOException {
		// TODO Auto-generated method stub
		return linereader.getPos();
	}

	@Override
	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		return linereader.getPos();
	}

	@Override
	public synchronized boolean next(LongWritable key, Text value) throws IOException {
		// TODO Auto-generated method stub
		
		boolean append, isNextLine=false;
		byte space [] = {' '};
		while(linereader.next(linekey, linevalue))
		{
			if(linevalue.toString().length() > 0)
			{
				byte [] rawbytes = linevalue.getBytes();
				int rawlinelength = linevalue.getLength();
				
				value.append(rawbytes, 0, rawlinelength);
				value.append(space, 0, 1);
				
				
			}
			isNextLine = true;
		}
		return isNextLine;
	}

	
  	
}
   public static class WordPair implements Writable{
     private int val1;
     private int val2;
     
     public WordPair()
     {
    	// val1 = new IntWritable();
    	 //val2 = new IntWritable();
    	 
     }
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		//val1.readFields(in);
		val1 = in.readInt();
		val2 = in.readInt();
		//val2.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		//val1.write(out);
		out.write(val1);
		//val2.write(out);
		out.write(val2);
		
	}
	  public void setkey(int val)
	  {
		  this.val1 = val;
	  }
	  public void setval(int val)
	  {
		  this.val2 = val;
	  }
   }
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text,IntWritable, WordPair>
    {
		private Text word = new Text();
    	IntWritable one = new IntWritable(1);
    	IntWritable len = new IntWritable();
		private MapWritable pair = new MapWritable();
		private  WordPair wordpair = new WordPair();
		//String [] words = new String[100];
		
		
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, WordPair> output, Reporter reporter)
				throws IOException {
			int count = 1;
			List<String> words = new ArrayList<String>();
			
			String line = value.toString();
		
			StringTokenizer tokenizer = new StringTokenizer(line);
			
			String token;
		    count = 1;
			words.clear();
			while(tokenizer.hasMoreTokens())
			{
				token = tokenizer.nextToken();
				//Matcher m = p.matcher(token);
				//String tok="";
				if(token.matches("^[a-zA-Z]+$"))
				{
				  //words[count] = token;
					words.add(token);
				  
				}
			}		
			//Iterator<String> wordlist = words.iterator();
			
			for (int i=0;i<words.size()/*words.length*/;i++)
			{
				for(int j=i+1;j<words.size()/*words.length*/;j++)
				{
					//String str = wordlist.next();
					
					//pair.put(new IntWritable(words[i].length()), new IntWritable(words[j].length()));
					
					int distance = (j+1)-(i+1);
					//pair.put(new IntWritable(words.get(j).length()), new IntWritable(distance));
					wordpair.setkey(words.get(j).length());
					wordpair.setval(distance);
					output.collect(new IntWritable(words.get(i).length()),wordpair);				
				}
			}
			
		}
			
			
			
			
	  }
    
	
	
	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, WordPair, IntWritable, WordPair>
    {
		//private MapWritable incrementingMap = new MapWritable();
		@Override
		public void reduce(IntWritable key, Iterator<WordPair> value,
				OutputCollector<IntWritable,WordPair> output, Reporter reporter)
				throws IOException {
			
			/*incrementingMap.clear();		
			while(value.hasNext())
			{
				//MapWritable val = value.next();
				
				Set<Writable> keys = val.keySet();
				for(Writable mykey:keys)
				{
					IntWritable fromcount = (IntWritable) val.get(mykey);
					incrementingMap.put(mykey, fromcount);
					
				}
				
			  
			}*/
			while(value.hasNext())
			    output.collect(key, value.next());
		}
    }
	
	
	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(WordDistance.class);

		// TODO: specify output types
		
      
		
		//conf.setMapOutputKeyClass(MapWritable.class);
		//conf.setMapOutputValueClass(IntWritable.class);
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(WordPair.class);
		
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf,new Path(args[1]));

		// TODO: specify a mapper
		conf.setMapperClass(Map.class);

		// TODO: specify a reducer
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(ParagraphParser.class);
	    conf.setOutputFormat(TextOutputFormat.class);

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
