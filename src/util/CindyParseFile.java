package util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter; 
import java.util.Date;
 

public class CindyParseFile {

	final static String cindy_stage_table = "cindy_stg";
	final static String pairDir = "/ifs/archive/shares/af_lab/CPTAC/ARACNe";
	final static String outDir = "/ifs/archive/shares/af_lab/CPTAC/ARACNe/parse_files";

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		try {
			 
			parseCindyFile("cindy-blca.txt", "cindy_blca", "v1.1");
			parseCindyFile( "cindy-brca.txt", "cindy_brca", "v1.1");
			parseCindyFile( "cindy-coad.txt", "cindy_coad", "v1.1");
			parseCindyFile( "cindy-gbm.txt", "cindy_gbm", "v1.1");
			parseCindyFile( "cindy-hnsc.txt", "cindy_hnsc", "v1.1");
			parseCindyFile( "cindy-kirc.txt", "cindy_kirc", "v1.1");
			parseCindyFile( "cindy-kirp.txt", "cindy_kirp", "v1.1");
			parseCindyFile( "cindy-laml.txt", "cindy_laml", "v1.1");
			parseCindyFile( "cindy-lgg.txt", "cindy_lgg", "v1.1");
			parseCindyFile( "cindy-lihc.txt", "cindy_lihc", "v1.1");
			parseCindyFile( "cindy-luad.txt", "cindy_luad", "v1.1");
			parseCindyFile( "cindy-lusc.txt", "cindy_lusc", "v1.1");
			parseCindyFile( "cindy-ov.txt", "cindy_ov", "v1.1");
			parseCindyFile( "cindy-prad.txt", "cindy_prad", "v1.1");
			parseCindyFile( "cindy-read.txt", "cindy_read", "v1.1");
			parseCindyFile( "cindy-sarc.txt", "cindy_sarc", "v1.1");
			parseCindyFile( "cindy-skcm.txt", "cindy_skcm", "v1.1");
			parseCindyFile( "cindy-stad.txt", "cindy_stad", "v1.1");
			parseCindyFile( "cindy-thca.txt", "cindy_thca", "v1.1");
			parseCindyFile( "cindy-ucec.txt", "cindy_ucec", "v1.1");

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void parseCindyFile(String pairFile,
			String interactome, String version) throws Exception {

		System.out.println("Parse " + pairFile + " process started at: "
				+ (new Date()).toString());
		File inFile = new File(pairDir, pairFile);
		File outFile = new File(outDir, cindy_stage_table + "." + pairFile);

		String header = "entrez_gene1 entrez_gene2 p_value interaction_type source source_version";
		PrintWriter out = new PrintWriter(new FileWriter(outFile));
		;
		out.println(header);
		BufferedReader cindyPairBufferedReader = new BufferedReader(
				new FileReader(inFile));
		cindyPairBufferedReader.readLine(); // skip first line
		String pairDataLine = cindyPairBufferedReader.readLine();
		while (pairDataLine != null) {
			String[] tokens1 = pairDataLine.trim().split("\t");
			if (tokens1.length < 4) {
				pairDataLine = cindyPairBufferedReader.readLine();
				continue;
			}

			out.append(tokens1[0].trim() + "\t");
			out.append(tokens1[1].trim() + "\t");
			out.append(tokens1[3].trim() + "\t");
			out.append("pp" + "\t");
			out.append(interactome + "\t");
			out.append(version + "\n");	
			pairDataLine = cindyPairBufferedReader.readLine();
		}// end while

		if (out != null)
			out.close();
		if (cindyPairBufferedReader != null)
			cindyPairBufferedReader.close();

		System.out.println("Finished " + pairFile + " process ended at: "
				+ (new Date()).toString());
	}

}
