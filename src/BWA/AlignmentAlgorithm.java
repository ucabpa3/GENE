package BWA;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * User: yukun
 * Date: 28/08/2013
 * Time: 15:01
 */
public interface AlignmentAlgorithm {
    /**
     *
     * @param context
     * @param input
     * @param outputFile
     */
    public void alignSingle( Mapper.Context context, String input, Path outputFile,long key) throws IOException, InterruptedException;

    /**
     *
     * @param context
     * @param input_1
     * @param input_2
     * @param outputFile
     */
    public void alignPaired(Mapper.Context context, String input_1, String input_2, Path outputFile, long key) throws IOException, InterruptedException;
}
