import java.io.IOException;
import java.util.StringTokenizer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.net.URI;
import java.io.BufferedReader;
import java.io.FileReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Covid19_3 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, FloatWritable>{

    private Text row = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      if(value.toString().contains("new_cases"))
        return;
      String[] covid_row = value.toString().split(",");
      FloatWritable num_cases = new FloatWritable(Integer.parseInt(covid_row[2]));
      Text loc = new Text(covid_row[1]);
      context.write(loc , num_cases);
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,FloatWritable,Text,FloatWritable> {

    HashMap<String, Integer> pop_map = new HashMap<String, Integer>();
    
    public void setup(Context context) throws IOException, InterruptedException{

      URI [] cfile = context.getCacheFiles();
      if (cfile != null && cfile.length > 0){
        BufferedReader reader;
        try{
          reader = new BufferedReader(new FileReader("pop_file"));
          int i =0;
          String line = "";
          while((line = reader.readLine()) != null){
            String[] pop_data = line.split(",");

            if(i!=0 && pop_data.length == 5){
              int j = Integer.parseInt(pop_data[4]);
              pop_map.put(pop_data[0].toString(), j);
            }
            i=1;
          }
        } catch(Exception e){ System.out.println(e);}
      }
      
    }

    public void reduce(Text key, Iterable<FloatWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {

      if(pop_map.containsKey(key.toString())){
        int pop = pop_map.get(key.toString());
        float sum =0;
        for (FloatWritable val : values) {
          sum += val.get();
        }
        float m = sum/pop* 1000000;
        FloatWritable result = new FloatWritable(m);

        context.write(key, result);
      }
      else
        return;    
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    
    String cpath = args[1].toString();

    Job job = Job.getInstance(conf, "covid 19_3");
    
    job.setJarByClass(Covid19_3.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    job.addCacheFile(new URI(cpath + "#pop_file"));

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}