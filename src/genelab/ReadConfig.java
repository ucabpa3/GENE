package genelab;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;

/**
 * @author Hao Chen
 */
public class ReadConfig {
    public static int N_LINES_PER_CHUNKS = 40;
    public static int MAX_LINE_LENGTH = Integer.MAX_VALUE;
    /* LOCAL PATHS */
    public static String PATH_MAIN = "/Users/yukun/" + "genelab/";
    public static String PATH_BWA = PATH_MAIN;
    public static String PATH_REFERENCE = PATH_MAIN + "reference/";
    /* HDFS PATHS */
    public static String BWAHDFS = "/user/costas/bwa";

    public static void read() {
        try {
            File f = new File("config.xml");
            DocumentBuilderFactory factory = DocumentBuilderFactory
                    .newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(f);
            NodeList nl = doc.getElementsByTagName("bwa-config");

            if (!Integer.valueOf(doc
                    .getElementsByTagName("N_LINES_PER_CHUNKS").item(0)
                    .getFirstChild().getNodeValue()).equals("")) {
                N_LINES_PER_CHUNKS = Integer.valueOf(doc
                        .getElementsByTagName("N_LINES_PER_CHUNKS").item(0)
                        .getFirstChild().getNodeValue());
            }

            if (!doc.getElementsByTagName("PATH_MAIN").item(0)
                    .getFirstChild().getNodeValue().equals("")) {
                PATH_MAIN = doc.getElementsByTagName("PATH_MAIN").item(0)
                        .getFirstChild().getNodeValue();
                PATH_BWA = PATH_MAIN;
            }

            if (!doc.getElementsByTagName("PATH_REFERENCE").item(0)
                    .getFirstChild().getNodeValue().equals("")) {
                PATH_REFERENCE = doc.getElementsByTagName("PATH_REFERENCE").item(0)
                        .getFirstChild().getNodeValue();
            }

            if (!doc.getElementsByTagName("BWAHDFS").item(0)
                    .getFirstChild().getNodeValue().equals("")) {
                BWAHDFS = doc.getElementsByTagName("BWAHDFS").item(0)
                        .getFirstChild().getNodeValue();
            }
            System.out.println(N_LINES_PER_CHUNKS);
            System.out.println(PATH_MAIN);
            System.out.println(PATH_BWA);
            System.out.println(PATH_REFERENCE);
            System.out.println(BWAHDFS);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}