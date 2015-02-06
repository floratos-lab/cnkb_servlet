package org.geworkbench.components.interactions.cellularnetwork;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.File;
import java.io.FileInputStream; 
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

import org.apache.log4j.Logger;

import java.sql.PreparedStatement;

public class InteractionsExport {

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
		logger.info("Start exporting....");
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

		logger.info("End exporting....");
	}

	public void getInteractionsSifFormat(int versionId, String interactionType,
			String shortName, String presentBy, PrintWriter out, Connection conn)
			throws SQLException {

		logger.info("Start exporting....");

		if (presentBy.equalsIgnoreCase(GENE_NAME))
			presentBy = GeneSymbolOnly;
		else if (presentBy.equalsIgnoreCase(GENE_ID))
			presentBy = EntrezIDPreferred;

		loadTargetRoleId(conn);

		out.println("sif format data");
		if (presentBy.endsWith("Only"))
			processSifListByOnly(versionId, interactionType, presentBy,
					shortName, out, conn);
		else
			processSifListByPreferred(versionId, interactionType, presentBy,
					shortName, out, conn);

		conn.close();
		logger.info("End exporting....");
	}

	public void getInteractionsAdjFormat(int versionId, String interactionType,
			String presentBy, PrintWriter out, Connection conn)
			throws SQLException {

		logger.info("Start exporting....");

		if (presentBy.equalsIgnoreCase(GENE_NAME))
			presentBy = GeneSymbolOnly;
		else if (presentBy.equalsIgnoreCase(GENE_ID))
			presentBy = EntrezIDPreferred;

		loadTargetRoleId(conn);
		out.println("adj format data");
		if (presentBy.endsWith("Only"))
			processAdjListByOnly(versionId, interactionType, presentBy, out,
					conn);
		else
			processAdjListByPreferred(versionId, interactionType, presentBy,
					out, conn);

		conn.close();
		logger.info("End exporting....");
	}

	private void processAdjListByOnly(int versionId, String interactionType,
			String presentBy, PrintWriter out, Connection exportConn)
			throws SQLException {
		String aSql = null;
		Map<String, List<GeneInfo>> iteractionMap = new HashMap<String, List<GeneInfo>>();

		if (presentBy.equalsIgnoreCase(EntrezIDOnly))
			aSql = "SELECT ijp.interaction_id, ijp.primary_accession, ijp.confidence_value, ijp.role_name as role_id ";
		else
			aSql = "SELECT ijp.interaction_id, ijp.gene_symbol, ijp.confidence_value, ijp.role_name as role_id ";

		aSql += "FROM interactions_joined_part_new ijp ";
		aSql += "WHERE ijp.interactome_version_id = " + versionId;
		if (!interactionType.equalsIgnoreCase("ALL")) {
			aSql += " and ijp.interaction_type ='" + interactionType + "'";
		}

		Statement stm = exportConn.createStatement();
		stm.setFetchSize(Integer.MIN_VALUE);
		ResultSet rs = stm.executeQuery(aSql);

		int previousInteractionId = 0;
		List<GeneInfo> geneList = new ArrayList<GeneInfo>(3);	 
		while (rs.next()) {
			String gene = null;
			if (presentBy.equalsIgnoreCase(EntrezIDOnly))
				gene = rs.getString("primary_accession");
			else
				gene = rs.getString("gene_symbol");
			if (gene == null || gene.trim().equals("null")
					|| gene.trim().equals("")
					|| gene.trim().equalsIgnoreCase("UNKNOWN"))
				continue;
			int currentInteractionId = rs.getInt("interaction_id");
			int role = rs.getInt("role_id");
			float cValue = rs.getFloat("confidence_value");
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

		stm.close();
		rs.close();
	}

	private void processAdjListByPreferred(int versionId,
			String interactionType, String presentBy, PrintWriter out,
			Connection exportConn) throws SQLException {
		String aSql = null;
		Map<String, List<GeneInfo>> iteractionMap = new HashMap<String, List<GeneInfo>>();

		aSql = "SELECT ijp.interaction_id, ijp.primary_accession, ijp.secondary_accession, ijp.gene_symbol, ijp.confidence_value, ijp.role_name as role_id ";
		aSql += "FROM interactions_joined_part_new ijp ";
		aSql += "WHERE ijp.interactome_version_id = " + versionId;
		if (!interactionType.equalsIgnoreCase("ALL")) {
			aSql += " and ijp.interaction_type ='" + interactionType + "'";
		}

		Statement stm = exportConn.createStatement();
		stm.setFetchSize(Integer.MIN_VALUE);
		ResultSet rs = stm.executeQuery(aSql);

		int previousInteractionId = 0;
		List<GeneInfo> geneList = new ArrayList<GeneInfo>(3);		 
		while (rs.next()) {
			String gene = null;
			if (presentBy.equalsIgnoreCase(EntrezIDPreferred)) {
				gene = rs.getString("primary_accession");
				if (gene == null || gene.trim().equals("")
						|| gene.trim().equals("null")
						|| gene.equalsIgnoreCase("UNKNOWN"))
					gene = rs.getString("secondary_accession");
				if (gene == null || gene.trim().equals("")
						|| gene.trim().equals("null")
						|| gene.equalsIgnoreCase("UNKNOWN"))
					gene = rs.getString("gene_symbol");
			} else {
				gene = rs.getString("gene_symbol");
				if (gene == null || gene.trim().equals("")
						|| gene.trim().equals("null")
						|| gene.equalsIgnoreCase("UNKNOWN"))
					gene = rs.getString("primary_accession");
				if (gene == null || gene.trim().equals("")
						|| gene.trim().equals("null")
						|| gene.equalsIgnoreCase("UNKNOWN"))
					gene = rs.getString("secondary_accession");
			}

			if (gene == null || gene.trim().equals("")
					|| gene.trim().equals("null")
					|| gene.equalsIgnoreCase("UNKNOWN"))
				continue;

			int currentInteractionId = rs.getInt("interaction_id");
			int role = rs.getInt("role_id");
			float cValue = rs.getFloat("confidence_value");
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

		stm.close();
		rs.close();

	}

	private void processSifListByOnly(int versionId, String interactionType,
			String presentBy, String shortName, PrintWriter out,
			Connection exportConn) throws SQLException {
		String aSql = null;
		Map<String, List<GeneInfo>> iteractionMap = new HashMap<String, List<GeneInfo>>();

		if (presentBy.equalsIgnoreCase(EntrezIDOnly))
			aSql = "SELECT ijp.interaction_id, ijp.primary_accession, ijp.role_name as role_id ";
		else
			aSql = "SELECT ijp.interaction_id, ijp.gene_symbol, ijp.role_name as role_id ";
		aSql += "FROM interactions_joined_part_new ijp ";
		aSql += "WHERE ijp.interactome_version_id = " + versionId;
		aSql += " and ijp.interaction_type ='" + interactionType + "'";

		Statement stm = exportConn.createStatement();
		stm.setFetchSize(Integer.MIN_VALUE);
		ResultSet rs = stm.executeQuery(aSql);

		int previousInteractionId = 0;
		List<GeneInfo> geneList = new ArrayList<GeneInfo>(3);
	 
		while (rs.next()) {
			String gene = null;
			if (presentBy.equalsIgnoreCase(EntrezIDOnly))
				gene = rs.getString("primary_accession");
			else
				gene = rs.getString("gene_symbol");
			if (gene == null || gene.trim().equals("null")
					|| gene.trim().equals("")
					|| gene.trim().equalsIgnoreCase("UNKNOWN"))
				continue;
			int currentInteractionId = rs.getInt("interaction_id");
			int role = rs.getInt("role_id");

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

		stm.close();
		rs.close();
	}

	private void processSifListByPreferred(int versionId,
			String interactionType, String presentBy, String shortName,
			PrintWriter out, Connection exportConn) throws SQLException {
		String aSql = null;
		Map<String, List<GeneInfo>> iteractionMap = new HashMap<String, List<GeneInfo>>();

		aSql = "SELECT ijp.interaction_id, ijp.primary_accession, ijp.secondary_accession, ijp.gene_symbol, ijp.role_name as role_id ";
		aSql += "FROM interactions_joined_part_new ijp ";
		aSql += "WHERE ijp.interactome_version_id = " + versionId;
		aSql += " and ijp.interaction_type ='" + interactionType + "' ";

		Statement stm = exportConn.createStatement();
		stm.setFetchSize(Integer.MIN_VALUE);
		ResultSet rs = stm.executeQuery(aSql);

		int previousInteractionId = 0;
		List<GeneInfo> geneList = new ArrayList<GeneInfo>(3);	 
		while (rs.next()) {
			String gene = null;
			if (presentBy.equalsIgnoreCase(EntrezIDPreferred)) {
				gene = rs.getString("primary_accession");
				if (gene == null || gene.trim().equals("")
						|| gene.trim().equals("null")
						|| gene.equalsIgnoreCase("UNKNOWN"))
					gene = rs.getString("gene_symbol");
				if (gene == null || gene.trim().equals("")
						|| gene.trim().equals("null")
						|| gene.equalsIgnoreCase("UNKNOWN"))
					gene = rs.getString("secondary_accession");
			} else {

				gene = rs.getString("gene_symbol");
				if (gene == null || gene.trim().equals("")
						|| gene.trim().equals("null")
						|| gene.equalsIgnoreCase("UNKNOWN"))
					gene = rs.getString("primary_accession");
				if (gene == null || gene.trim().equals("")
						|| gene.trim().equals("null")
						|| gene.equalsIgnoreCase("UNKNOWN"))
					gene = rs.getString("secondary_accession");
			}

			if (gene == null || gene.trim().equals("")
					|| gene.trim().equals("null")
					|| gene.equalsIgnoreCase("UNKNOWN"))
				continue;

			int currentInteractionId = rs.getInt("interaction_id");
			int role = rs.getInt("role_id");
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

		stm.close();
		rs.close();

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
		for (int i = 0; i < geneList.size(); i++) {
			if (geneList.get(i).role == TARGET_ROLE_ID)
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

	private static void loadTargetRoleId(Connection conn) throws SQLException {
		if (TARGET_ROLE_ID == 0) {
			String aSql = null;
			aSql = "SELECT id FROM role where name = '" + TARGET + "'";
			;
			PreparedStatement statement = conn.prepareStatement(aSql);
			ResultSet rs = statement.executeQuery();
			while (rs.next()) {

				// Get the data from the row using the column name
				TARGET_ROLE_ID = rs.getInt("id");
				break;
			}
			rs.close();
			statement.close();
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

	private class GeneInfo {
		String gene = null;
		float confidence;
		int role;

		GeneInfo(String gene, float confidence, int role) {
			this.gene = gene;
			this.confidence = confidence;
			this.role = role;
		}

	}

}
