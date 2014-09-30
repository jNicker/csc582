import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PushCountDriver {

  public static void main(String[] args) throws Exception {
    Configuration configuration = new Configuration();
    Job job = Job.getInstance(configuration, "Push Count");
    job.setJarByClass(PushCountDriver.class);
    job.setMapperClass(PushCountMapper.class);
    job.setCombinerClass(PushCountReducer.class);
    job.setReducerClass(PushCountReducer.class);
    job.setNumReduceTasks(1);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    //setTextOutputFormatSeparator(job, ":");
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  static void setTextOutputFormatSeparator(final Job job, final String separator) {
    final Configuration conf = job.getConfiguration();
    conf.set("mapred.textoutputformat.separator", separator);
    conf.set("mapreduce.textoutputformat.separator", separator);
    conf.set("mapreduce.output.textoutputformat.separator", separator);
    conf.set("mapreduce.output.key.field.separator", separator);
    conf.set("mapred.textoutputformat.separatorText", separator);
  }

}

