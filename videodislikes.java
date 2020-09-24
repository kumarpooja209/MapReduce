

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

public class videodislikes {

  public static class videodislikesMapper
       extends Mapper<LongWritable, Text, Text, IntWritable>{
	   
	private Text video_id = new Text();
    private static IntWritable dislikes = new IntWritable();
    

    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
					String line=value.toString();
					String str[]=line.split(",");
					video_id.set(str[0]);

					int i =Integer.parseInt(str[7]);
					dislikes.set(i);


        context.write(video_id, dislikes);
      }
    }
	
	
	  public static class videodislikesReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
	  int sum = 0;
	  for (IntWritable val : values) {
        sum += val.get();
      }

	  context.write(key,new IntWritable(sum));
	  
    }
  }
  
  

 

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "YouTube");
    job.setJarByClass(videodislikes.class);
    job.setMapperClass(videodislikesMapper.class);
    job.setCombinerClass(videodislikesReducer.class);
    job.setReducerClass(videodislikesReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

