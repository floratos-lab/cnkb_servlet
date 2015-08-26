package org.geworkbench.components.interactions.cellularnetwork;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
  
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

import org.apache.log4j.Logger; 
 
import com.mongodb.AggregationOutput; 
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

public class InteractionsMongoExport {

	private static final String TARGET = "target";

	private static final String GENE_ID = "gene id";
	private static final String GENE_NAME = "gene name";

	private static final String EntrezIDOnly = "Entrez ID Only";
	private static final String GeneSymbolOnly = "Gene Symbol Only";

	private static final String EntrezIDPreferred = "Entrez ID Preferred";
	private static final String GeneSymbolPreferred = "Gene Symbol Preferred";
	private static final String GET_INTERACTIONS_SIF_FORMAT = "getInteractionsSifFormat";

	private static String datafileDir = null;
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger
			.getLogger(InteractionsExport.class);

	private static int TARGET_ROLE_ID = 0;

	static {
		datafileDir = System.getProperty("user.home")
				+ System.getProperty("file.separator") + "cnkb_export_data"
				+ System.getProperty("file.separator");
	}

	public void exportExistFile(String fileName, PrintWriter out) {
		logger.info("Start export existing file ......");
		File inDocFile = new File(fileName);
		InputStream inStream = null;
		if (fileName.endsWith("adj"))
			out.println("adj format data");
		else
			out.println("sif format data");
		try {
			inStream = new FileInputStream(inDocFile);
			 BufferedReader br=new BufferedReader(new InputStreamReader(inStream));
			 String line = br.readLine();
			while (line != null) {
				out.println(line);
				line =  br.readLine();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				inStream.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		logger.info("End export existing file ......");
	}

	public void getInteractionsSifFormat(int versionId, String interactionType,
			String shortName, String presentBy, PrintWriter out, DB db)
			throws MongoException {

		logger.info("Start exporting....");

		if (presentBy.equalsIgnoreCase(GENE_NAME))
			presentBy = GeneSymbolOnly;
		else if (presentBy.equalsIgnoreCase(GENE_ID))
			presentBy = EntrezIDPreferred;

		loadTargetRoleId(db);

		out.println("sif format data");
		if (presentBy.endsWith("Only"))
			processSifListByOnly(versionId, interactionType, presentBy,
					shortName, out, db);
		else
			processSifListByPreferred(versionId, interactionType, presentBy,
					shortName, out, db);

		 
		logger.info("End exporting....");
	}

	public void getInteractionsAdjFormat(int versionId, String interactionType,
			String presentBy, PrintWriter out, DB db)
			throws MongoException {

		logger.info("Start exporting....");

		if (presentBy.equalsIgnoreCase(GENE_NAME))
			presentBy = GeneSymbolOnly;
		else if (presentBy.equalsIgnoreCase(GENE_ID))
			presentBy = EntrezIDPreferred;

		loadTargetRoleId(db);
		out.println("adj format data");
		if (presentBy.endsWith("Only"))
			processAdjListByOnly(versionId, interactionType, presentBy, out,
					db);
		else
			processAdjListByPreferred(versionId, interactionType, presentBy,
					out, db);
		 
		logger.info("End exporting....");
	}

	private void processAdjListByOnly(int versionId, String interactionType,
			String presentBy, PrintWriter out, DB db)
			throws MongoException {
	
		DBCollection collection = db.getCollection("interactions_denorm");
		BasicDBObject whereQuery = new BasicDBObject();
		whereQuery.put("interactome_version_id", versionId);
		if (!interactionType.equalsIgnoreCase("ALL"))
			whereQuery.put("interaction_type", interactionType);		 
	 
		BasicDBObject fields = new BasicDBObject();
		fields.put("interaction_id", 1);
		if (presentBy.equalsIgnoreCase(EntrezIDOnly))
			fields.put("primary_accession", 1);
		else
			fields.put("gene_symbol", 1);
		fields.put("confidence_value", 1);
		fields.put("role_name", 1);
		fields.put("_id", 0);
		 
		
	    DBCursor dbCursor = collection.find(whereQuery, fields).sort(new BasicDBObject(
	     	"interaction_id", 1));		
		 
		Map<String, List<GeneInfo>> iteractionMap = new HashMap<String, List<GeneInfo>>();		
		int previousInteractionId = 0;
		List<GeneInfo> geneList = new ArrayList<GeneInfo>(3);	 
		while (dbCursor.hasNext()) {
			DBObject result = dbCursor.next();	
			String gene = null;
			if (presentBy.equalsIgnoreCase(EntrezIDOnly))
				gene = result.get("primary_accession").toString();
			else
				gene = result.get("gene_symbol").toString();
			if (gene == null || gene.trim().equals("null") || gene.trim().equals("\\N") 
					|| gene.trim().equals("")
					|| gene.trim().equalsIgnoreCase("UNKNOWN"))
				continue;
			int currentInteractionId = ((Number)result.get("interaction_id")).intValue();
			int role = ((Number)result.get("role_name")).intValue();
			Float cValue = new Float(result.get("confidence_value").toString());
			if (previousInteractionId != currentInteractionId) {
				if (geneList.size() > 1) {
					processOneInteraction(iteractionMap, geneList);
				}

				geneList.clear();
				previousInteractionId = currentInteractionId;
				geneList.add(new GeneInfo(gene, cValue, role));

			} else {
				geneList.add(new GeneInfo(gene, cValue, role));

			}
 
		}

		if (geneList.size() > 1) {
			processOneInteraction(iteractionMap, geneList);
		}
		if (iteractionMap.size() > 0) {
			sendAdjInteractions(iteractionMap, out);
		}

		 
	}
	
	 

	private void processAdjListByPreferred(int versionId,
			String interactionType, String presentBy, PrintWriter out,
			DB db) throws MongoException {
		
		DBCollection collection = db.getCollection("interactions_denorm");
		BasicDBObject whereQuery = new BasicDBObject();
		whereQuery.put("interactome_version_id", versionId);
		if (!interactionType.equalsIgnoreCase("ALL"))
			whereQuery.put("interaction_type", interactionType);
	 	
		BasicDBObject fields = new BasicDBObject();
		fields.put("interaction_id", 1);	 
		fields.put("primary_accession", 1);
		fields.put("secondary_accession", 1);		
		fields.put("gene_symbol", 1);
		fields.put("confidence_value", 1);
		fields.put("role_name", 1);
		fields.put("_id", 0);		
	    DBCursor dbCursor = collection.find(whereQuery, fields).sort(new BasicDBObject(
	     	"interaction_id", 1));
		
		Map<String, List<GeneInfo>> iteractionMap = new HashMap<String, List<GeneInfo>>();		 
		int previousInteractionId = 0;
		List<GeneInfo> geneList = new ArrayList<GeneInfo>(3);		 
		while (dbCursor.hasNext()) {
			DBObject result = dbCursor.next();	
			String gene = null;
			if (presentBy.equalsIgnoreCase(EntrezIDPreferred)) {
				gene = result.get("primary_accession").toString();
				if (gene.trim().equals("") || gene.trim().equals("\\N")
						|| gene.trim().equals("null")
						|| gene.equalsIgnoreCase("UNKNOWN"))
					gene = result.get("gene_symbol").toString();
				if (gene == null || gene.trim().equals("") || gene.trim().equals("\\N")
						|| gene.trim().equals("null")
						|| gene.equalsIgnoreCase("UNKNOWN"))
					gene = result.get("secondary_accession").toString();
			} else {
				gene = result.get("gene_symbol").toString();
				if (gene.trim().equals("") || gene.trim().equals("\\N")
						|| gene.trim().equals("null")
						|| gene.equalsIgnoreCase("UNKNOWN"))
					gene = result.get("primary_accession").toString();
				if (gene == null || gene.trim().equals("") || gene.trim().equals("\\N")
						|| gene.trim().equals("null")
						|| gene.equalsIgnoreCase("UNKNOWN"))
					gene = result.get("secondary_accession").toString();
			}

			if (gene == null || gene.trim().equals("")
					|| gene.trim().equals("null")
					|| gene.equalsIgnoreCase("UNKNOWN"))
				continue;

			int currentInteractionId = ((Number)result.get("interaction_id")).intValue();
			int role = (Integer)result.get("role_name");
			Float cValue = new Float(result.get("confidence_value").toString());
			if (previousInteractionId != currentInteractionId) {
				if (geneList.size() > 1) {
					processOneInteraction(iteractionMap, geneList);
				}

				geneList.clear();
				previousInteractionId = currentInteractionId;
				geneList.add(new GeneInfo(gene, cValue, role));

			} else {
				geneList.add(new GeneInfo(gene, cValue, role));
			}
			 
		}

		if (geneList.size() > 1) {
			processOneInteraction(iteractionMap, geneList);
		}
		if (iteractionMap.size() > 0) {
			sendAdjInteractions(iteractionMap, out);
		}	 
	}

	private void processSifListByOnly(int versionId, String interactionType,
			String presentBy, String shortName, PrintWriter out,
			DB db) throws MongoException {		

		DBCollection collection = db.getCollection("interactions_denorm");
		BasicDBObject whereQuery = new BasicDBObject();
		whereQuery.put("interactome_version_id", versionId);		 
		whereQuery.put("interaction_type", interactionType);	 	
		BasicDBObject fields = new BasicDBObject();
		fields.put("interaction_id", 1);
		if (presentBy.equalsIgnoreCase(EntrezIDOnly))
			fields.put("primary_accession", 1);
		else
			fields.put("gene_symbol", 1);		 
		fields.put("role_name", 1);
		fields.put("_id", 0);
		
		DBCursor dbCursor = collection.find(whereQuery, fields).sort(new BasicDBObject(
			     	"interaction_id", 1));		 
		Map<String, List<GeneInfo>> iteractionMap = new HashMap<String, List<GeneInfo>>();	 
		int previousInteractionId = 0;
		List<GeneInfo> geneList = new ArrayList<GeneInfo>(3);
	 
		while (dbCursor.hasNext()) {
			DBObject result = dbCursor.next();	
			String gene = null;
			if (presentBy.equalsIgnoreCase(EntrezIDOnly))
				gene = result.get("primary_accession").toString();
			else
				gene = result.get("gene_symbol").toString();
			if (gene == null || gene.trim().equals("null") || gene.trim().equals("\\N")
					|| gene.trim().equals("")
					|| gene.trim().equalsIgnoreCase("UNKNOWN"))
				continue;
			int currentInteractionId = ((Number)result.get("interaction_id")).intValue();
			int role = (Integer)result.get("role_name");

			if (previousInteractionId != currentInteractionId) {
				if (geneList.size() > 1) {
					processOneInteraction(iteractionMap, geneList);
				}
				geneList.clear();
				previousInteractionId = currentInteractionId;
				geneList.add(new GeneInfo(gene, 0, role));
			} else {
				geneList.add(new GeneInfo(gene, 0, role));

			} 

		}

		if (geneList.size() > 1) {
			processOneInteraction(iteractionMap, geneList);
		}
		if (iteractionMap.size() > 0) {
			sendSifInteractions(iteractionMap, shortName, out);
		}
 
	}

	private void processSifListByPreferred(int versionId,
			String interactionType, String presentBy, String shortName,
			PrintWriter out, DB db) throws MongoException {
		
		DBCollection collection = db.getCollection("interactions_denorm");
		BasicDBObject whereQuery = new BasicDBObject();
		whereQuery.put("interactome_version_id", versionId);	 
		whereQuery.put("interaction_type", interactionType);
		 
		BasicDBObject fields = new BasicDBObject();
		fields.put("interaction_id", 1);	 
		fields.put("primary_accession", 1);
		fields.put("secondary_accession", 1);		
		fields.put("gene_symbol", 1);		 
		fields.put("role_name", 1);
		fields.put("_id", 0);
		
	    DBCursor dbCursor = collection.find(whereQuery, fields).sort(new BasicDBObject(
	     	"interaction_id", 1));
		 
		Map<String, List<GeneInfo>> iteractionMap = new HashMap<String, List<GeneInfo>>(); 
		int previousInteractionId = 0;
		List<GeneInfo> geneList = new ArrayList<GeneInfo>(3);	 
		while (dbCursor.hasNext()) {
			DBObject result = dbCursor.next();	
			String gene = null;
			if (presentBy.equalsIgnoreCase(EntrezIDPreferred)) {
				gene = result.get("primary_accession").toString();
				if (gene == null || gene.trim().equals("") || gene.trim().equals("\\N")
						|| gene.trim().equals("null")
						|| gene.equalsIgnoreCase("UNKNOWN"))
					gene = result.get("gene_symbol").toString();
				if (gene == null || gene.trim().equals("") || gene.trim().equals("\\N")
						|| gene.trim().equals("null")
						|| gene.equalsIgnoreCase("UNKNOWN"))
					gene = result.get("secondary_accession").toString();
			} else {

				gene = result.get("gene_symbol").toString();
				if (gene == null || gene.trim().equals("")
						|| gene.trim().equals("null")
						|| gene.equalsIgnoreCase("UNKNOWN"))
					gene = result.get("primary_accession").toString();
				if (gene == null || gene.trim().equals("")
						|| gene.trim().equals("null")
						|| gene.equalsIgnoreCase("UNKNOWN"))
					gene = result.get("secondary_accession").toString();
			}

			if (gene == null || gene.trim().equals("")
					|| gene.trim().equals("null")
					|| gene.equalsIgnoreCase("UNKNOWN"))
				continue;

			int currentInteractionId = ((Number)result.get("interaction_id")).intValue();
			int role = (Integer)result.get("role_name");
			if (previousInteractionId != currentInteractionId) {
				if (geneList.size() > 1) {
					processOneInteraction(iteractionMap, geneList);
				}

				geneList.clear();
				previousInteractionId = currentInteractionId;
				geneList.add(new GeneInfo(gene, 0, role));
			} else {
				geneList.add(new GeneInfo(gene, 0, role));

			}
 
		}

		if (geneList.size() > 1) {
			processOneInteraction(iteractionMap, geneList);
		}
		if (iteractionMap.size() > 0) {
			sendSifInteractions(iteractionMap, shortName, out);
		}
 

	}

	private void sendAdjInteractions(Map<String, List<GeneInfo>> iteractionMap,
			PrintWriter out) {
		InteractionsConnectionImplServlet.printMemoryUsage();
		List<String> keyList = new ArrayList<String>(iteractionMap.keySet());
		Collections.sort(keyList, String.CASE_INSENSITIVE_ORDER);
		for (int i = 0; i < keyList.size(); i++) {
			List<GeneInfo> itList = iteractionMap.get(keyList.get(i));
			out.print(keyList.get(i));
			for (int j = 0; j < itList.size(); j++) {
				out.print("\t" + itList.get(j).gene + "\t"
						+ itList.get(j).confidence);
			}
			out.println();
		}
		iteractionMap.clear();

	}

	private void sendSifInteractions(Map<String, List<GeneInfo>> iteractionMap,
			String shortName, PrintWriter out) {
		InteractionsConnectionImplServlet.printMemoryUsage();
		List<String> keyList = new ArrayList<String>(iteractionMap.keySet());
		Collections.sort(keyList, String.CASE_INSENSITIVE_ORDER);
		for (int i = 0; i < keyList.size(); i++) {
			List<GeneInfo> itList = iteractionMap.get(keyList.get(i));
			out.print(keyList.get(i) + "\t" + shortName);
			for (int j = 0; j < itList.size(); j++) {
				out.print("\t" + itList.get(j).gene);

			}
			out.println();
		}
		iteractionMap.clear();

	}

	private void processOneInteraction(
			Map<String, List<GeneInfo>> iteractionMap, List<GeneInfo> geneList) {
		Collections.sort(geneList);
		for (int i = 0; i < geneList.size(); i++) {
			if (geneList.get(0).role == TARGET_ROLE_ID)
				continue;
			List<GeneInfo> newGeneList = new ArrayList<GeneInfo>();
			for (int j = 0; j < geneList.size(); j++) {
				if (j == i)
					continue;
				GeneInfo geneInfo = geneList.get(j);
				newGeneList.add(new GeneInfo(geneInfo.gene,
						geneInfo.confidence, geneInfo.role));
			}

			if (!iteractionMap.containsKey(geneList.get(i).gene))
				iteractionMap.put(geneList.get(i).gene, newGeneList);
			else {
				iteractionMap.get(geneList.get(i).gene).addAll(newGeneList);
			}

			break;

		}
	}
 

	String getExportExistFileName(String context, String version, String shortName,
			String presentBy, String methodName) {
		String name = null;
		StringBuffer sb = new StringBuffer(datafileDir + "export_" + context + "_"
				+ version);
		if (shortName != null && !shortName.trim().equals(""))
			sb.append("_" + shortName);
		else
			sb.append("_all");
		if (presentBy.equalsIgnoreCase(GeneSymbolOnly)
				|| presentBy.equalsIgnoreCase(GENE_NAME))
			sb.append("_GSO");
		else if (presentBy.equalsIgnoreCase(EntrezIDOnly))
			sb.append("_EIO");
		else if (presentBy.equalsIgnoreCase(EntrezIDPreferred)
				|| presentBy.equalsIgnoreCase(GENE_ID))
			sb.append("_EIP");
		else
			sb.append("_GSP");
		if (methodName.equalsIgnoreCase(GET_INTERACTIONS_SIF_FORMAT))
			sb.append(".sif");
		else
			sb.append(".adj");

		name = sb.toString();
		if (new File(name).exists())
			return name;
		else
			return null;

	}
	
	private static void loadTargetRoleId(DB db) throws MongoException {
		 
		if (TARGET_ROLE_ID == 0) {
			DBCollection collection = db.getCollection("role");
			BasicDBObject whereQuery = new BasicDBObject();
			whereQuery.put("name", TARGET);
			DBObject match = new BasicDBObject("$match", whereQuery);
			DBObject fields = new BasicDBObject();		 
			fields.put("id", 1);
			fields.put("_id", 0);
			DBObject project = new BasicDBObject("$project", fields);
			List<DBObject> pipeline = Arrays.asList(match, project);
			AggregationOutput output = collection.aggregate(pipeline);
			for (DBObject result : output.results()) {		
				
				  TARGET_ROLE_ID = new Double(result.get("id").toString()).intValue();
				  break;
			}
		}

	}
	

	private class GeneInfo implements Comparable<GeneInfo>{
		String gene = null;
		float confidence;
		int role;

		GeneInfo(String gene, float confidence, int role) {
			this.gene = gene;
			this.confidence = confidence;
			this.role = role;
		}
		
		public int compareTo(GeneInfo compareGeneInfo) {
			 
			int compareRole = ((GeneInfo) compareGeneInfo).getRole(); 
	 
			//ascending order
			return this.role - compareRole;
	 
			 
		}
		
		int getRole()
		{
			return role;
		}

	}

}