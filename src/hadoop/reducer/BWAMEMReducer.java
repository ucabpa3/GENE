package hadoop.reducer;

import genelab.Assistant;
import inputFormat.FQSplitInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * User: yukun
 * Date: 12/07/2013
 * Time: 14:02
 */

public class BWAMEMReducer extends Reducer<LongWritable, FQSplitInfo, String, String> {


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        context.setStatus("copying bwa");
        context.progress();
        Assistant.copyBWA(context.getConfiguration());
        context.setStatus("copying reference");
        context.progress();
        Assistant.copyReference(context.getConfiguration());
    }

    @Override
    public void reduce(LongWritable key, Iterable<FQSplitInfo> value, Context context) {
//        Configuration conf = context.getConfiguration();
//        Assistant.log("start key " + key, context);
//        File workingDir = new File(Conf.PATH_CACHE + context.getJobID().toString() + "_" + key);
////        System.out.println(workingDir.getAbsolutePath());
//        if (new File(workingDir.getAbsolutePath()).mkdirs()) {
//            System.out.println("created the local working directory");
//        }
//
//        context.setStatus("writing down input files");
//        context.progress();
////        write down .fq files
////            Date start = new Date();
//        String outputPath[] = new String[2];
//        for (FQSplitInfo info : value) {
//            Path inFile = new Path(info.getPath());
//            String outFile = workingDir.getAbsolutePath() + "/" + inFile.getName() + "_" + key + ".fq";
//            if (outputPath[0] == null) {
//                outputPath[0] = outFile;
//            } else {
//                outputPath[1] = outFile;
//            }
//            // Read from and write to new file
//            FileSystem fs = null;
//            try {
//                fs = FileSystem.get(conf);
//                FSDataInputStream in = fs.open(inFile);
//                in.seek(info.getStart());
//                File file = new File(outFile);
//                if (!file.exists()) {
//                    file.createNewFile();
//                } else {
//                    file.delete();
//                    file.createNewFile();
//                }
//                FileOutputStream outputStream = new FileOutputStream(file);
//                byte[] bytes = new byte[4096];
//                for (int n = 0; n < info.getLength() / 4096; n++) {
//                    in.read(bytes);
//                    outputStream.write(bytes, 0, 4096);
//                }
//                if (in.read(bytes) != -1) {
//                    outputStream.write(bytes, 0, (int) (info.getLength() % 4096));
//                }
//                in.close();
//                outputStream.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//
//        }
//        Arrays.sort(outputPath);
//
//        context.setStatus("running bwa");
//        context.progress();
//        BWAmem algorithm = new BWAmem();
//        try {
//            algorithm.alignPaired(context, outputPath[0], outputPath[1], new Path(FileOutputFormat.getOutputPath(context) + "/temp/" + key));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        Assistant.log("finish key " + key, context);
//
//        //clean working directory
//        context.setStatus("cleaning");
//        context.progress();
//        Assistant.deleteDir(workingDir);
//
//        context.setStatus("appending");
//        context.progress();
//        Assistant.appendResult(conf);
//        context.setStatus("finish");
//        context.progress();

    }


}
