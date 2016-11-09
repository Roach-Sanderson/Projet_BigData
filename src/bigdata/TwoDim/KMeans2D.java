/**
 * @author David Auber 
 * @date 07/10/2016
 * Ma�tre de conf�rences HDR
 * LaBRI: Universit� de Bordeaux
 */

package bigdata.TwoDim;
import org.apache.hadoop.util.ProgramDriver;

public class KMeans2D {
	
	public static void main( String[] args ) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}