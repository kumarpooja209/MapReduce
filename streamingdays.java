//stremingdays


import java.io.IOException;
import java.text.ParseException;
import java.util.StringTokenizer;
import java.io.*;



import java.util.concurrent.TimeUnit;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
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

public class streamingdays {

  public static class streamingdaysMapper
       extends Mapper<LongWritable, Text, Text, IntWritable>{
	   
	private Text title= new Text();

	private static IntWritable days = new IntWritable();
	
    
	
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {

					String line=value.toString();
					String str[]=line.split(","); 
					
					title.set(str[2]);
					try
                                         {
					SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
                                        Date s1 = sdf.parse(str[1]);
					Date s2=sdf.parse(str[14]);

				//	String d1=str[1];
				//	String d2=str[14];
				//	days = d1.getAs[Timestamp]-d2.getAs[Timestamp];
				//	val d1 = dateFormatter.parse(str[1]);
				//	val d2 = dateFormatter.parse(str[14]);
				//	long diffDays= s1.getTime()-s2.getTime();
				//	int temp=(int)diffDays;
				//	days.set(temp);

//String s1=str[1].replace("/","-");
//String s2=str[14].replace("/","-");

//SimpleDateFormat formatter = new SimpleDateFormat("yyyy/mm/dd");
//String f1=formatter.format(s1);
//String f2= formatter.format(s2);
//LocalDate d1 = LocalDate.parse(f1, DateTimeFormatter.ISO_LOCAL_DATE);
//LocalDate d2 = LocalDate.parse(f2, DateTimeFormatter.ISO_LOCAL_DATE);
//Duration diff = Duration.between(s1.getTime(), s2.getTime());
long diff = Math.abs(s1.getTime() - s2.getTime());
long diffDays = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
 

//long diffDays = diff.toDays();	
int temp=(int)diffDays;
days.set(temp);
              //                            Date date1=new SimpleDateFormat("dd/MM/yyyy").parse(d1);  					
					




        context.write(title ,days);
}
catch(ParseException ex)
{
}
      }
    }
	
	
	  public static class streamingdaysReducer
       extends Reducer<Text,IntWritable,IntWritable, Text> {
	
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
	  int count=0;
	  for (IntWritable val : values) {
	  
        count += val.get();
      }

	  context.write(new IntWritable(count),key);
	  
    }
  }
  
  

 

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "YouTube");
    job.setJarByClass(streamingdays.class);
    job.setMapperClass(streamingdaysMapper.class);
    job.setCombinerClass(streamingdaysReducer.class);
    job.setReducerClass(streamingdaysReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

