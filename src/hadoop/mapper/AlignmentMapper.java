package hadoop.mapper;

import BWA.AlignmentAlgorithm;
import BWA.BWAbacktrack;
import BWA.BWAmem;
import genelab.Assistant;
import genelab.Conf;
import inputFormat.FQSplitInfo;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

/**
 * User: yukun
 * Date: 29/08/2013
 * Time: 19:16
 */
public class AlignmentMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    AlignmentAlgorithm algorithm;
    long key;
    String output;
    File workingDir;
    FileSystem fs;
    boolean finished;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        fs = FileSystem.get(context.getConfiguration());
        finished = false;
        Runtime.getRuntime().addShutdownHook(new Clean());
        if (context.getConfiguration().get("algorithm").equals("mem")) {
            algorithm = new BWAmem();
        } else if (context.getConfiguration().get("algorithm").equals("backtrack")) {
            algorithm = new BWAbacktrack();
        }

        context.setStatus("copying bwa");
        context.progress();
        Assistant.copyBWA(context.getConfiguration());
        context.setStatus("copying reference");
        context.progress();
        Assistant.copyReference(context.getConfiguration());
    }

    public void map(LongWritable k, Text value, Context context) throws InterruptedException, IOException {
//        String skey = ""+value.toString();
        key = Long.parseLong(value.toString());
        output = context.getConfiguration().get("outputPath") + "/temp/" + context.getTaskAttemptID();
        workingDir = new File(Conf.PATH_CACHE + context.getTaskAttemptID().toString());
        workingDir.mkdirs();
        context.nextKeyValue();
        FQSplitInfo split1 = new FQSplitInfo(context.getCurrentValue().toString());
        Path inFile = new Path(split1.getPath());

        String outFile = workingDir.getAbsolutePath() + "/" + inFile.getName();
        while (!new File(outFile).exists() || new File(outFile).length() != split1.getLength()) {
            Assistant.log("info: " + split1.getLength() + " file: " + new File(outFile).length(), context);
            Assistant.copyChunk(split1, outFile);
        }
        if (context.nextKeyValue()) {
            String[] outputPath = new String[2];
            outputPath[0] = outFile;
            FQSplitInfo split2 = new FQSplitInfo(context.getCurrentValue().toString());
            inFile = new Path(split2.getPath());
            outputPath[1] = workingDir.getAbsolutePath() + "/" + inFile.getName();
            while (!new File(outputPath[1]).exists() || new File(outputPath[1]).length() != split2.getLength()) {
                Assistant.log("info: " + split2.getLength() + " file: " + new File(outputPath[1]).length(), context);
                Assistant.copyChunk(split2, outputPath[1]);
            }
            Arrays.sort(outputPath);
            context.setStatus("alignment paired");
            context.progress();
            try {
                algorithm.alignPaired(context, outputPath[0], outputPath[1], new Path(output),key);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            context.setStatus("alignment single");
            context.progress();
            try {
                algorithm.alignSingle(context, outFile, new Path(output),key);
            } catch (IOException e) {
                e.printStackTrace();
            }
            finished = true;
//        clean working directory
            context.setStatus("cleaning");
            context.progress();
            Assistant.deleteDir(workingDir);
            Assistant.deleteDir(new File("/opt/cores/"));
            context.write(new LongWritable(key), new Text(output));
//            context.setStatus("appending");
//            context.progress();
//            Assistant.appendResult(context.getConfiguration());
        }
    }

    public class Clean extends Thread {
        public Clean() {
            super("clean");
        }

        public void run() {
            if (workingDir != null) {
                Assistant.deleteDir(workingDir);
                Assistant.deleteDir(new File("/opt/cores/"));
            }
        }
    }

}
