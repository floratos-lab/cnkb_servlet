package util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter; 
import java.util.Date;
 


//please change the proper value of the "inputDir" and "outDir". 
public class AracneParseFile {

	final static String aracne_stage_table = "aracne_stg" ;
	final static String inputDir = "/ifs/archive/shares/af_lab/CPTAC/ARACNe";
	final static String outDir = "/ifs/archive/shares/af_lab/CPTAC/ARACNe/parse_files";
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		try {			
			parseAracneFile("aracne-blca.txt", "aracne_blca", "v1.1");
			parseAracneFile("aracne-brca.txt", "aracne_brca", "v1.1");
			parseAracneFile("aracne-coad.txt", "aracne_coad", "v1.1");
			parseAracneFile("aracne-gbm.txt", "aracne_gbm", "v1.1");
			parseAracneFile("aracne-hnsc.txt", "aracne_hnsc", "v1.1");
			parseAracneFile("aracne-kirc.txt", "aracne_kirc", "v1.1");
			parseAracneFile("aracne-kirp.txt", "aracne_kirp", "v1.1");
			parseAracneFile("aracne-laml.txt", "aracne_laml", "v1.1");
			parseAracneFile("aracne-lgg.txt", "aracne_lgg", "v1.1");
			parseAracneFile("aracne-lihc.txt", "aracne_lihc", "v1.1");
			parseAracneFile("aracne-luad.txt", "aracne_luad", "v1.1");
			parseAracneFile("aracne-lusc.txt", "aracne_lusc", "v1.1");
			parseAracneFile("aracne-ov.txt", "aracne_ov", "v1.1");
			parseAracneFile("aracne-prad.txt", "aracne_prad", "v1.1");
			parseAracneFile("aracne-read.txt", "aracne_read", "v1.1");
			parseAracneFile("aracne-sarc.txt", "aracne_sarc", "v1.1");
			parseAracneFile("aracne-skcm.txt", "aracne_skcm", "v1.1");
			parseAracneFile("aracne-stad.txt", "aracne_stad", "v1.1");
			parseAracneFile("aracne-thca.txt", "aracne_thca", "v1.1");
			parseAracneFile("aracne-ucec.txt", "aracne_ucec", "v1.1");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	
	
	public static void parseAracneFile(String aracneInFile, String interactome, String version) throws Exception {

		System.out.println("Parse aracne file process start at: "
				+ (new Date()).toString());
		File inFile = new File(inputDir, aracneInFile);		 	
		File outFile = new File(inputDir, aracne_stage_table + "." + aracneInFile);
	 
	 
		String header = "hub	hub_xref	target	target_xref	MI	MoA	likelihood	pvalue	interactome 	version	interaction_type";
		PrintWriter out = new PrintWriter(new FileWriter(outFile));; 
		out.println(header);
		BufferedReader aracneBufferedReader = new BufferedReader(new FileReader(inFile)); 
	    String dataLine = aracneBufferedReader.readLine();
	    
		while (dataLine != null ) {
			if (dataLine.trim().equals("") || dataLine.startsWith("Hub"))
			{
				dataLine = aracneBufferedReader.readLine(); 
				continue;			
			}
			String[] tokens = dataLine.trim().split("\t");			
			out.append(tokens[0].trim() +"\t");
			out.append("Entrez Gene\t" ); 
			out.append(tokens[1].trim() +"\t");
			out.append("Entrez Gene\t" ); 			 
			out.append(tokens[2].trim() + "\t");
			out.append(tokens[3].trim() + "\t");
			out.append(tokens[4].trim() + "\t");
			out.append(tokens[5].trim() + "\t");
			out.append(interactome + "\t");
			out.append(version + "\t");		 
			out.append("protein-dna\n");
			dataLine = aracneBufferedReader.readLine(); 
			  
	    }// end while
 
		if (out != null)
			out.close();
		if (aracneBufferedReader != null)
	    	aracneBufferedReader.close();

		System.out.println("Parse aracne file process end at: "
				+ (new Date()).toString());
	}
	

}
