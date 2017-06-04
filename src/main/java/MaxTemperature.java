import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperature {

    public static class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {  // here need to extends FloatWritable type
      @Override
      public void map( LongWritable key, Text value, Context context )
          throws IOException, InterruptedException {

        String line = value.toString( );   //  transfer current Text to String type
        String locationDay = line.substring( 0, 16 ); // extract current key
        String currentTimeString;
        //String year = line.substring(0,4);
        float airTemperature; // temperature , type is float
        Date currentTimeDate = new Date( );
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat( "HH:mm:ss" ); // transfer string represent Date to really Date type, for determining whether current record time between 18:00:00 and 05:00:00

        try {
          line.concat( "0" ); // because string to float will lose accuracy, we need to avoid lose accuracy according to concat one character
          airTemperature = Float.parseFloat( line.substring( 31, line.length() ).trim( ) ); // transfer string type temperature to float type

          currentTimeString = line.substring( 17, 24 );  // extract current time string
          //System.out.println( currentTimeString );
          currentTimeDate = simpleDateFormat.parse( currentTimeString ); // transfer current time variable type from string to Date
          if ( currentTimeDate.after( simpleDateFormat.parse( "18:00:00" ) ) && currentTimeDate // determine whether the time is between 18:00:00 and 05:00:00 next
              .before( simpleDateFormat.parse( "23:59:59" ) ) ) {
            //System.out.println( "Date compare success!" );
            context.write( new Text( locationDay ), new FloatWritable( airTemperature ) );
          }else if( currentTimeDate.after( simpleDateFormat.parse( "00:00:00" ) ) && currentTimeDate
              .before( simpleDateFormat.parse( "05:00:00" ) ) ){
            context.write( new Text( locationDay ), new FloatWritable( airTemperature ) );  // write to the context
          }else{ }

        } catch ( Exception e ) {
          e.printStackTrace( );
        }
      }
    }

    public static class MaxTemperatureReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> { // here need to extends FloatWritable type

      @Override
      public void reduce(Text key, Iterable<FloatWritable> values, Context context)
          throws IOException, InterruptedException{

        float maxValue = Float.MIN_VALUE;
        for(FloatWritable value : values){  // iterator all values belong to the key, choose the max value and output the key-value
          maxValue = Math.max( maxValue, value.get() );
        }
        context.write( key, new FloatWritable( maxValue ) );
      }

    }

    public static void main(String[] args) throws Exception{
      if (args.length!= 2)
      {
        System.err.println("Usage: MinTemperature<input path> <output path>");
        System.exit(-1);
      }
      //Job job= new Job();
      //Job job = Job.getInstance(Configuration conf);
      //Job job = Job.getInstance(getConf(), "MinTemperatureMapRedurce");
      Job job = Job.getInstance();
      job.setJarByClass(MaxTemperature.class);
      job.setJobName("Min temperature");
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      job.setMapperClass(MaxTemperatureMapper.class);
      job.setReducerClass(MaxTemperatureReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(FloatWritable.class);
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

  }

