package bigdata;



import org.apache.hadoop.util.ProgramDriver;

public class BigData {
	public static void main( String[] args ) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("1d", bigdata.TwoDim.KMeans1DIt.class, "creates a kmeans");
			pgd.addClass("nd", bigdata.TwoDim.KMeansND.class, "creates a kmeans");
			pgd.addClass("hier", bigdata.TwoDim.KMeansHier.class, "creates a kmeans");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}