package sandbox;

import genelab.Conf;
import inputFormat.FQSplitInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * User: yukun
 * Date: 06/07/2013
 * Time: 01:57
 */
@SuppressWarnings("ALL")
public class test {
    public static void main(String[] args) throws Exception {
        Date start = new Date();
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        System.out.println("job start at: "+dateFormat.format(start)+"\n");
        Date end = new Date();
        System.out.println(end.getTime()-start.getTime());
//        Configuration conf = new Configuration();
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//        if (otherArgs.length != 2) {
//            System.err.println("Usage: test <in> <out>");
//            System.exit(2);
//        }
//        conf.set("mapreduce.input.lineinputformat.linespermap", Conf.N_LINES_PER_CHUNKS + "");
//        Job job = new Job(conf, "test");
//        job.setJarByClass(test.class);
//        job.setMapperClass(MyMapper.class);
//        job.setReducerClass(MyReducer.class);
//        job.setInputFormatClass(FQInputFormat.class);
////        job.setInputFormatClass(FQNLineInputFormat2.class);
//        job.setOutputKeyClass(LongWritable.class);
//        job.setOutputValueClass(FQSplitInfo.class);
//        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
//        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    @SuppressWarnings("unchecked")
    public static class MyMapper extends Mapper<LongWritable, FQSplitInfo, LongWritable, FQSplitInfo> {
        private static String v;
        private static long k;

//        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
//            v = "";
//            k = ((LongWritable) context.getCurrentKey()).get();
////            v=context.getInputSplit()
//        }

        public void map(LongWritable key, FQSplitInfo value, Context context) throws IOException, InterruptedException {
//            System.out.println(value);
//            v = v.concat(value.toString()).concat("\n");
//            System.out.println(key);
//            System.out.println(value);
            context.write(key, value);
        }

//        @Override
//        protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {
//            //noinspection unchecked
//            context.write(new LongWritable(k), new Text(v.substring(0, v.length() - 1)));
//        }

    }

    public static class MyReducer extends Reducer<LongWritable, FQSplitInfo, LongWritable, Text> {


        public void reduce(LongWritable key, Iterable<FQSplitInfo> values, Context context) throws IOException, InterruptedException {

//            System.out.println("key: "+key);
//            System.out.println("value: ");
            for (FQSplitInfo info : values) {
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(conf);

                // Hadoop DFS deals with Path
                Path inFile = new Path(info.getPath());
//                Path outFile = new Path(Conf.PATH_MAIN + context.getJobID().toString() + "_" + key + "/" + inFile.getName() + "_" + key + ".fq");
                String outFile = Conf.PATH_MAIN + inFile.getName() + "_" + key + ".txt";
                System.out.println(outFile);

                // Read from and write to new file
                FSDataInputStream in = fs.open(inFile);
                in.seek(info.getStart());
                File file = new File(outFile);
                if (!file.exists()) {
                    file.createNewFile();
                } else {
                    file.delete();
                }
                FileOutputStream outputStream = new FileOutputStream(file);
                int read = 0;
                byte[] bytes = new byte[1048576];
                for (int n = 0; n < info.getLength() / 1048576; n++) {
                    in.read(bytes);
                    outputStream.write(bytes, 0, 1048576);
                }
                if ((read = in.read(bytes)) != -1) {
                    outputStream.write(bytes, 0, (int) info.getLength() % 1048576);
                }
                in.close();
                outputStream.close();
            }
        }


    }
}
