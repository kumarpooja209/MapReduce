

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

public class channelviews {

  public static class channelviewsMapper
       extends Mapper<LongWritable, Text, Text, LongWritable>{
	   
	private Text channel_title = new Text();
    private static LongWritable views = new LongWritable();
    

    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
					String line=value.toString();
					String str[]=line.split(",");
					channel_title.set(str[3]);
					

					int i =Integer.parseInt(str[5]);
					views.set(i);


        context.write(channel_title, views);
      }
    }
	
	
	  public static class channelviewsReducer
       extends Reducer<Text,LongWritable,Text,LongWritable> {

    public void reduce(Text key, Iterable<LongWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
	  int sum = 0;
	  for (LongWritable val : values) {
        sum += val.get();
      }

	  context.write(key, new LongWritable(sum));
	  
    }
  }
  
  

 

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "YouTube");
    job.setJarByClass(channelviews.class);
    job.setMapperClass(channelviewsMapper.class);
    job.setCombinerClass(channelviewsReducer.class);
    job.setReducerClass(channelviewsReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

