package BWA;

import genelab.Conf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * User: yukun
 * Date: 05/08/2013
 * Time: 14:56
 */
public class Maintainer {


    public static void cleanAll() throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("clean path", Conf.PATH_MAIN);
        run(conf);
    }

    public static void cleanBWA() throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("clean path", Conf.PATH_BWA);
        run(conf);
    }

    public static void cleanRef() throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("clean path", Conf.PATH_REFERENCE);
        run(conf);
    }

    public static void cleanCache() throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("clean path", Conf.PATH_CACHE);
        run(conf);
    }

    public static void run(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
        FileSystem fs = FileSystem.get(conf);
        fs.create(new Path(Conf.HDFS_TEMP + "input/"));
        Job job = new Job(conf, "clean " + conf.get("clean path"));
        job.setJarByClass(Maintainer.class);
        job.setMapperClass(MaintainerMapper.class);
        job.setReducerClass(MaintainerReducer.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setNumReduceTasks(12);
        FileInputFormat.addInputPath(job, new Path(Conf.HDFS_TEMP + "input/"));
        FileOutputFormat.setOutputPath(job, new Path(Conf.HDFS_TEMP + "output"));
        if (job.waitForCompletion(true)) {
            fs.delete(new Path(Conf.HDFS_TEMP), true);
        }
        System.exit(0);
    }

    public static class MaintainerMapper extends Mapper<Object, Object, Object, Object> {
    }

    public static class MaintainerReducer extends Reducer<Object, Object, Object, Object> {
        protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
                throws IOException,
                InterruptedException {
            Assistant.deleteDir(new File(context.getConfiguration().get("clean path")));
        }

    }
}
