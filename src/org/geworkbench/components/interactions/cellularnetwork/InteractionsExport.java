package org.geworkbench.components.interactions.cellularnetwork;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.ArrayList;

import org.apache.log4j.Logger;

public class InteractionsExport {

	private static final String TARGET = "target";
	private static final String MODULATOR = "modulator";

	private static final String GENE_ID = "gene id";
	private static final String GENE_NAME = "gene name";

	private static final String EntrezIDOnly = "Entrez ID Only";
	private static final String GeneSymbolOnly = "Gene Symbol Only";

	private static final String EntrezIDPreferred = "Entrez ID Preferred";
	private static final String GeneSymbolPreferred = "Gene Symbol Preferred";

	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger
			.getLogger(InteractionsExport.class);

	public void getInteractionsSifFormat(int versionId, String interactionType,
			String presentBy, PrintWriter out, Connection conn)
			throws SQLException {

		logger.info("Start exporting....");

		if (presentBy.equalsIgnoreCase(GENE_NAME))
			presentBy = GeneSymbolOnly;
		else if (presentBy.equalsIgnoreCase(GENE_ID))
			presentBy = EntrezIDPreferred;

		String aSql = null;

		PreparedStatement prepStat = null;

		List<GeneInfo> tfList = getRelatedInteractionGenes(versionId,
				interactionType, presentBy, conn);

		out.println("sif format data");

		for (GeneInfo tfGene : tfList) {

			if (tfGene.gene == null || tfGene.gene.trim().equals("")
					|| tfGene.gene.equalsIgnoreCase("UNKNOWN")) {
				logger.info("drop gene ...." + tfGene);
				continue;
			}
			logger.debug("Start get ...." + tfGene.gene);
			String idList = getInteractionIds(tfGene, interactionType,
					versionId, prepStat, conn);

			aSql = "SELECT pe.primary_accession, pe.secondary_accession, pe.gene_symbol, it.short_name, r.name ";
			aSql += "FROM physical_entity pe, interaction_participant ip, interaction i, interaction_type it, role r ";
			aSql += "WHERE i.id in (" + idList + ") ";
			aSql += "AND pe.id=ip.participant_id ";
			aSql += "AND ip.interaction_id=i.id ";
			aSql += "AND i.interaction_type=it.id ";
			aSql += "AND ip.role_id = r.id ";
			aSql += "order by it.short_name, pe.gene_symbol";

			prepStat = conn.prepareStatement(aSql);

			ResultSet rs2 = prepStat.executeQuery();

			String targetGene = null;
			String shortName = null;
			String previousShortName = null;
			String roleName = null;

			boolean hasData = false;
			while (rs2.next()) {
				if (presentBy.equalsIgnoreCase(GeneSymbolOnly)) {
					targetGene = rs2.getString("gene_symbol");
				} else if (presentBy.equalsIgnoreCase(GeneSymbolPreferred)) {
					targetGene = rs2.getString("gene_symbol");
					if (targetGene == null || targetGene.trim().equals("")
							|| targetGene.trim().equalsIgnoreCase("UNKNOWN"))
						targetGene = rs2.getString("primary_accession");
					if (targetGene == null || targetGene.trim().equals("")
							|| targetGene.trim().equalsIgnoreCase("UNKNOWN"))
						targetGene = rs2.getString("secondary_accession");
				} else if (presentBy.equalsIgnoreCase(EntrezIDOnly))
					targetGene = rs2.getString("primary_accession");
				else if (presentBy.equalsIgnoreCase(EntrezIDPreferred)) {
					targetGene = rs2.getString("primary_accession");
					if (targetGene == null || targetGene.trim().equals("")
							|| targetGene.trim().equalsIgnoreCase("UNKNOWN"))
						targetGene = rs2.getString("secondary_accession");
				}

				shortName = rs2.getString("short_name");
				roleName = rs2.getString("name");
 
				if (targetGene == null || targetGene.equals(tfGene.gene)
						|| targetGene.trim().equalsIgnoreCase("UNKNOWN")
						|| roleName.equals(MODULATOR))
					continue;
				if (previousShortName == null) {
					hasData = true;
					out.print(tfGene.gene + "\t" + shortName + "\t"
							+ targetGene);
				} else if (previousShortName.equalsIgnoreCase(shortName)) {
					out.print("\t" + targetGene);
				} else {
					out.println();
					out.print(tfGene.gene + "\t" + shortName + "\t"
							+ targetGene);

				}
				previousShortName = shortName;
			}

			if (hasData)
				out.println();
			rs2.close();
			prepStat.close();

		}

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

		String aSql = null;

		PreparedStatement prepStat = null;

		List<GeneInfo> tfList = getRelatedInteractionGenes(versionId,
				interactionType, presentBy, conn);

		out.println("adj format data");

		for (GeneInfo tfGene : tfList) {
             
			if (tfGene.gene == null || tfGene.gene.trim().equals("")
					|| tfGene.gene.equalsIgnoreCase("UNKNOWN")) {
				logger.info("drop gene ...." + tfGene);
				continue;
			}
			logger.debug("Start get ...." + tfGene.gene);
			String idList = getInteractionIds(tfGene, interactionType,
					versionId, prepStat, conn);
			logger.debug("get idlist");
			aSql = "SELECT pe.primary_accession, pe.secondary_accession, pe.gene_symbol, r.name, ic.score ";
			aSql += "FROM (physical_entity pe, interaction_participant ip, interaction i,role r) ";
			aSql += "left join interaction_confidence ic on (i.id=ic.interaction_id) ";
			aSql += "WHERE i.id in (" + idList + ") ";
			aSql += "AND pe.id=ip.participant_id ";
			aSql += "AND ip.interaction_id=i.id ";
			aSql += "AND ip.role_id = r.id ";
			aSql += "order by pe.gene_symbol";

			prepStat = conn.prepareStatement(aSql);
			ResultSet rs2 = prepStat.executeQuery();

			String targetGene = null;
			String confidence = null;
			String roleName = null;
			boolean hasData = false;

			while (rs2.next()) {

				if (presentBy.equalsIgnoreCase(GeneSymbolOnly)) {
					targetGene = rs2.getString("gene_symbol");
				} else if (presentBy.equalsIgnoreCase(GeneSymbolPreferred)) {
					targetGene = rs2.getString("gene_symbol");
					if (targetGene == null || targetGene.trim().equals("")
							|| targetGene.equalsIgnoreCase("UNKNOWN"))
						targetGene = rs2.getString("primary_accession");
					if (targetGene == null || targetGene.trim().equals("")
							|| targetGene.equalsIgnoreCase("UNKNOWN"))
						targetGene = rs2.getString("secondary_accession");
				} else if (presentBy.equalsIgnoreCase(EntrezIDOnly))
					targetGene = rs2.getString("primary_accession");
				else if (presentBy.equalsIgnoreCase(EntrezIDPreferred)) {
					targetGene = rs2.getString("primary_accession");
					if (targetGene == null || targetGene.trim().equals("")
							|| targetGene.equalsIgnoreCase("UNKNOWN"))
						targetGene = rs2.getString("secondary_accession");
				}

				
				roleName = rs2.getString("name");

				if (targetGene == null || targetGene.trim().equals("")
						|| targetGene.trim().equalsIgnoreCase("UNKNOWN")
						|| targetGene.equals(tfGene.gene)
						|| roleName.equals(MODULATOR))
					continue;				
				
				confidence = rs2.getString("score");
				if (hasData == false) {
					out.print(tfGene.gene + "\t" + targetGene + "\t"
							+ confidence);
					hasData = true;
				} else
					out.print("\t" + targetGene + "\t" + confidence);

			}
			if (hasData)
				out.println();
			rs2.close();
			prepStat.close();
		}

		logger.info("End exporting....");
	}

	private List<GeneInfo> getRelatedInteractionGenes(int versionId,
			String interactionType, String presentBy, Connection exportConn)
			throws SQLException {

		if (presentBy.equalsIgnoreCase(EntrezIDPreferred))
			return getRelatedGenesByEntrezIdPref(versionId, interactionType,
					exportConn);
		else if (presentBy.equalsIgnoreCase(GeneSymbolPreferred))
			return getRelatedGenesBySymbolPref(versionId, interactionType,
					exportConn);
		else if (presentBy.equalsIgnoreCase(EntrezIDOnly))
			return getRelatedGenesByEntrezIdOnly(versionId, interactionType,
					exportConn);
		else
			return getRelatedGenesBySymbolOnly(versionId, interactionType,
					exportConn);

	}

	private List<GeneInfo> getRelatedGenesBySymbolOnly(int versionId,
			String interactionType, Connection exportConn) throws SQLException {
		String aSql = null;
		List<GeneInfo> tfList = new ArrayList<GeneInfo>();

		aSql = "SELECT pe.gene_symbol, it.short_name ";
		aSql += "FROM interaction_interactome_version iiv, physical_entity pe, interaction_participant ip, interaction i, interaction_type it, role r ";
		aSql += "WHERE pe.id=ip.participant_id ";
		aSql += "AND ip.interaction_id=i.id ";
		aSql += "AND i.id = iiv.interaction_id ";
		aSql += "AND r.name <> '" + TARGET + "' ";
		aSql += "AND ip.role_id = r.id ";
		aSql += "And iiv.interactome_version_id = " + versionId + " ";
		if (!interactionType.equalsIgnoreCase("ALL")) {
			aSql += "AND it.id = i.interaction_type ";
			aSql += "and it.name ='" + interactionType + "' ";
		}

		aSql += "group by pe.gene_symbol";

		Statement stm = exportConn.createStatement();
		ResultSet rs = stm.executeQuery(aSql);
		while (rs.next())
			tfList.add(new GeneInfo(rs.getString("gene_symbol")));

		stm.close();
		rs.close();

		return tfList;
	}

	private List<GeneInfo> getRelatedGenesBySymbolPref(int versionId,
			String interactionType, Connection exportConn) throws SQLException {
		String aSql = null;
		List<GeneInfo> tfList = null;

		tfList = getRelatedGenesBySymbolOnly(versionId, interactionType,
				exportConn);

		aSql = "SELECT pe.primary_accession, pe.secondary_accession, it.short_name ";
		aSql += "FROM interaction_interactome_version iiv, physical_entity pe, interaction_participant ip, interaction i, interaction_type it, role r ";
		aSql += "WHERE "
				+ "(pe.gene_symbol is null OR pe.gene_symbol='UNKNOWN') ";
		aSql += "AND pe.id=ip.participant_id ";
		aSql += "AND ip.interaction_id=i.id ";
		aSql += "AND i.id = iiv.interaction_id ";
		aSql += "AND r.name <> '" + TARGET + "' ";
		aSql += "AND ip.role_id = r.id ";
		aSql += "And iiv.interactome_version_id = " + versionId + " ";
		if (!interactionType.equalsIgnoreCase("ALL")) {
			aSql += "AND it.id = i.interaction_type ";
			aSql += "and it.name ='" + interactionType + "' ";
		}
		aSql += "group by pe.primary_accession, pe.secondary_accession";

		Statement stm = exportConn.createStatement();
		ResultSet rs = stm.executeQuery(aSql);

		if (tfList == null)
			tfList = new ArrayList<GeneInfo>();

		while (rs.next()) {
			String gene = rs.getString("primary_accession");
			if (gene == null || gene.trim().equals("")
					|| gene.trim().equals("null"))
				tfList.add(new GeneInfo(rs.getString("secondary_accession"),
						false));
			else
				tfList.add(new GeneInfo(gene, true));
		}

		stm.close();
		rs.close();

		return tfList;
	}

	private List<GeneInfo> getRelatedGenesByEntrezIdOnly(int versionId,
			String interactionType, Connection exportConn) throws SQLException {
		String aSql = null;
		List<GeneInfo> tfList = new ArrayList<GeneInfo>();

		aSql = "SELECT pe.primary_accession,it.short_name ";
		aSql += "FROM interaction_interactome_version iiv, physical_entity pe, interaction_participant ip, interaction i, interaction_type it, role r ";
		aSql += "WHERE pe.primary_accession is not null ";
		aSql += "AND pe.id=ip.participant_id ";
		aSql += "AND ip.interaction_id=i.id ";
		aSql += "AND i.id = iiv.interaction_id ";
		aSql += "AND r.name <> '" + TARGET + "' ";
		aSql += "AND ip.role_id = r.id ";
		aSql += "And iiv.interactome_version_id = " + versionId + " ";
		if (!interactionType.equalsIgnoreCase("ALL")) {
			aSql += "AND it.id = i.interaction_type ";
			aSql += "and it.name ='" + interactionType + "' ";
		}

		aSql += "group by pe.primary_accession ";

		Statement stm = exportConn.createStatement();
		ResultSet rs = stm.executeQuery(aSql);

		while (rs.next()) {
			String gene = rs.getString("primary_accession");
			tfList.add(new GeneInfo(gene, true));
		}

		stm.close();
		rs.close();

		return tfList;
	}

	private List<GeneInfo> getRelatedGenesByEntrezIdPref(int versionId,
			String interactionType, Connection exportConn) throws SQLException {
		String aSql = null;
		List<GeneInfo> tfList = null;

		tfList = getRelatedGenesByEntrezIdOnly(versionId, interactionType,
				exportConn);

		aSql = "SELECT pe.secondary_accession, it.short_name ";
		aSql += "FROM interaction_interactome_version iiv, physical_entity pe, interaction_participant ip, interaction i, interaction_type it, role r ";
		aSql += "WHERE  pe.secondary_accession is not null ";
		aSql += "AND pe.id=ip.participant_id ";
		aSql += "AND ip.interaction_id=i.id ";
		aSql += "AND i.id = iiv.interaction_id ";
		aSql += "AND r.name <> '" + TARGET + "' ";
		aSql += "AND ip.role_id = r.id ";
		aSql += "And iiv.interactome_version_id = " + versionId + " ";
		if (!interactionType.equalsIgnoreCase("ALL")) {
			aSql += "AND it.id = i.interaction_type ";
			aSql += "and it.name ='" + interactionType + "' ";
		}

		aSql += "group by pe.secondary_accession";

		Statement stm = exportConn.createStatement();
		ResultSet rs = stm.executeQuery(aSql);

		while (rs.next())
			tfList.add(new GeneInfo(rs.getString("secondary_accession"), false));

		stm.close();
		rs.close();

		return tfList;
	}

	private String getInteractionIds(GeneInfo tfGene, String interactionType,
			int versionId, PreparedStatement prepStat, Connection conn)
			throws SQLException {
		String aSql, colName;

		if (tfGene.isPrimary == null)
			colName = "gene_symbol";
		else if (tfGene.isPrimary.booleanValue() == true)
			colName = "primary_accession";
		else
			colName = "secondary_accession";

		aSql = "SELECT i.id ";
		aSql += "FROM interaction_interactome_version iiv, physical_entity pe, interaction_participant ip, interaction i, interaction_type it ";
		aSql += "WHERE pe." + colName + " = ? ";
		aSql += "AND iiv.interactome_version_id = ? ";
		if (!interactionType.equalsIgnoreCase("ALL")) {
			aSql += "and it.name ='" + interactionType + "' ";
			aSql += "AND it.id = i.interaction_type ";
		}
		aSql += "AND ip.interaction_id=i.id ";
		aSql += "AND i.id = iiv.interaction_id ";
		aSql += "AND pe.id=ip.participant_id  ";

		prepStat = conn.prepareStatement(aSql);
		prepStat.setString(1, tfGene.gene);
		prepStat.setInt(2, versionId);

		ResultSet rs1 = prepStat.executeQuery();

		String idList = "";
		while (rs1.next()) {
			if (!idList.trim().equals(""))
				idList += ", ";
			idList += rs1.getString("id");

		}
		rs1.close();
		prepStat.close();

		return idList;
	}

	private class GeneInfo {
		String gene = null;
		Boolean isPrimary = null;

		GeneInfo(String gene, Boolean isPrimary) {
			this.gene = gene;
			this.isPrimary = isPrimary;
		}

		GeneInfo(String gene) {
			this.gene = gene;
			this.isPrimary = null;
		}
	}

}
