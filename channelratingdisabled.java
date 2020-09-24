//# In channel for how many videos ratings is diabled



import java.io.IOException;
import java.util.StringTokenizer;
import java.io.*;




import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class channelratingdisabled {

  public static class channelratingdisabledMapper
       extends Mapper<LongWritable, Text, Text, IntWritable>{
	   
	private Text channel = new Text();
    private final static IntWritable one = new IntWritable(1);
    

    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
					String line=value.toString();
					String str[]=line.split(",");
					
					if(str[10].matches("TRUE")){
					channel.set(str[3]);
       context.write(channel, one);
		}
      }
    }
	
	
	  public static class channelratingdisabledReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
	  int sum = 0;
	  for (IntWritable val : values) {
        sum += val.get();
      }

	  context.write(key, new IntWritable(sum));
	  
    }
  }
  
  

 

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "YouTube");
    job.setJarByClass(channelratingdisabled.class);
    job.setMapperClass(channelratingdisabledMapper.class);
    job.setCombinerClass(channelratingdisabledReducer.class);
    job.setReducerClass(channelratingdisabledReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

