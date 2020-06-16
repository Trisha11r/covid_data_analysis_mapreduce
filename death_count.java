import java.io.IOException;
import java.util.StringTokenizer;
import java.text.ParseException;
import java.lang.RuntimeException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Covid19_2 {

  public static class MyException
      extends RuntimeException{
        public MyException(String e){
          super(e);
        }
      }

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text row = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
      format.setLenient(false);

      String[] covid_row = value.toString().split(",");

      String fd, td, d_val;
      
      Date fdate = new Date();
      Date tdate= new Date();
      Date date_val = new Date();
      Date first_date = new Date();
      Date last_date = new Date();

      try{
        fd = conf.get("from_date");
        fdate = format.parse(fd);  
        td = conf.get("to_date");
        tdate = format.parse(td);  
        d_val = covid_row[0];
        date_val = format.parse(d_val);
        

      }catch(ParseException e){System.out.println(e); }

      if(value.toString().contains("new_deaths"))
        return;

      int start = date_val.compareTo(fdate);
      int end = date_val.compareTo(tdate);

      if(start>=0 && end<=0){
        IntWritable num_cases = new IntWritable(Integer.parseInt(covid_row[3]));
        Text loc = new Text(covid_row[1]);
        context.write(loc , num_cases);
      }      
      
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception, ParseException {
    Configuration conf = new Configuration();
    String f = args[1].toString();
    String t = args[2].toString(); 
    String fc = "2019-12-01";
    String lc = "2020-04-30";
    
    SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");
    
    sdformat.setLenient(false);
    try {

      Date dt_f = sdformat.parse(f);
      Date dt_t = sdformat.parse(t);
      Date dt_fc = sdformat.parse(fc);
      Date dt_lc = sdformat.parse(lc);

      if (dt_f.before(dt_fc) || dt_t.after(dt_lc))
        throw new MyException("Invalid Date Format: Dates not in the range 2019-12-01 to 2020-04-30"); 

    }
    catch (ParseException e) { System.out.println(e);
      System.exit(0);} 
    catch (MyException e) { System.out.println(e);
      System.exit(0);}
    


    conf.set("from_date", f);
    conf.set("to_date", t);

    Job job = Job.getInstance(conf, "covid 19_2");
    
    job.setJarByClass(Covid19_2.class);
    
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[3]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}