import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class Top {
    public static void main(String[] args) throws Exception {
        // args: input file / output file / topN  / keyAttribute Index
        String in = args[0];
        String out = args[1];

        final Logger logger = Logger.getLogger(Top.class);

        if (args.length > 2) {
            logger.info("Set topN to " + Integer.parseInt(args[2]));
            kaola.Config.topN = Integer.parseInt(args[2]);
            logger.info("Set topN to " + kaola.Config.topN);
        }

        if (args.length > 3) {
            logger.info("Set keyAttributeIdx to " + Integer.parseInt(args[3]));
            kaola.Config.keyAttributeIdx = Integer.parseInt(args[3]);
            logger.info("Set keyAttributeIdx to " + kaola.Config.keyAttributeIdx);
        }

        String ocurrenceOut = "/kaola/order/intermediate";
        String[] counterArgs = {in, ocurrenceOut};
        int exitCode = ToolRunner.run(new kaola.TopCounter(), counterArgs);
        if (exitCode != 0) {
            System.exit(exitCode);
        }

        String[] sortArgs = {ocurrenceOut, out};
        System.exit(ToolRunner.run(new kaola.TopSort(), sortArgs));
    }
}
