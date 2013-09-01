package sandbox;

import hadoop.mapper.AlignmentMapper;
import hadoop.reducer.AlignmentReducer;
import genelab.Conf;
import inputFormat.FQSplitInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;

/**
 * User: yukun
 * Date: 06/07/2013
 * Time: 01:57
 */
@SuppressWarnings("ALL")
public class test {
    public static void main(String[] args) throws Exception {
//         new File(Conf.PATH_CACHE+"123").mkdirs();
        Configuration conf = new Configuration();
        String output = Conf.HDFS_OUTPUT + "test";
        conf.set("reference", "138M");
        conf.set("outputPath", output);
        conf.set("mapreduce.input.lineinputformat.linespermap","3");
        Job job = new Job(conf, "test");
        job.setJarByClass(test.class);
        job.setMapperClass(AlignmentMapper.class);
        job.setReducerClass(AlignmentReducer.class);
        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/user/yukun/index/118MB"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
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

////            System.out.println("key: "+key);
////            System.out.println("value: ");
//            for (FQSplitInfo info : values) {
//                Configuration conf = new Configuration();
//                FileSystem fs = FileSystem.get(conf);
//
//                // Hadoop DFS deals with Path
//                Path inFile = new Path(info.getPath());
////                Path outFile = new Path(Conf.PATH_MAIN + context.getJobID().toString() + "_" + key + "/" + inFile.getName() + "_" + key + ".fq");
//                String outFile = Conf.PATH_MAIN + inFile.getName() + "_" + key + ".txt";
//                System.out.println(outFile);
//
//                // Read from and write to new file
//                FSDataInputStream in = fs.open(inFile);
//                in.seek(info.getStart());
//                File file = new File(outFile);
//                if (!file.exists()) {
//                    file.createNewFile();
//                } else {
//                    file.delete();
//                }
//                FileOutputStream outputStream = new FileOutputStream(file);
//                int read = 0;
//                byte[] bytes = new byte[4096];
//                for (int n = 0; n < info.getLength() / 4096; n++) {
//                    in.read(bytes);
//                    outputStream.write(bytes, 0, 4096);
//                }
//                if ((read = in.read(bytes)) != -1) {
//                    outputStream.write(bytes, 0, (int) info.getLength() % 4096);
//                }
//                in.close();
//                outputStream.close();
//            }
        }


    }
}
