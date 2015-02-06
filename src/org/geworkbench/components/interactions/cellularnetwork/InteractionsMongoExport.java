package org.geworkbench.components.interactions.cellularnetwork;

import java.io.PrintWriter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.mongodb.AggregationOutput; 
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

public class InteractionsMongoExport {

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
			.getLogger(InteractionsMongoExport.class);

	public void getInteractionsSifFormat(int versionId, String interactionType,
			String presentBy, PrintWriter out, DB db) throws MongoException {

		logger.info("Start exporting....");

		if (presentBy.equalsIgnoreCase(GENE_NAME))
			presentBy = GeneSymbolOnly;
		else if (presentBy.equalsIgnoreCase(GENE_ID))
			presentBy = EntrezIDPreferred;		
	 
		HashMap<String, String> interactionTypeMap = getInteractionTypeMap(db);
        
		List<GeneInfo> tfList = getRelatedInteractionGenes(versionId,
				interactionType, presentBy, db);

		out.println("sif format data");

		for (GeneInfo tfGene : tfList) {

			if (tfGene.gene == null || tfGene.gene.trim().equals("")
					|| tfGene.gene.equalsIgnoreCase("UNKNOWN")) {
				logger.info("drop gene ...." + tfGene);
				continue;
			}
			logger.info("Start get ...." + tfGene.gene);
			List<Integer> idList = getInteractionIds(tfGene, interactionType,
					versionId, db);

			/*
			 * aSql =
			 * "SELECT pe.primary_accession, pe.secondary_accession, pe.gene_symbol, it.short_name, r.name "
			 * ; aSql +=
			 * "FROM physical_entity pe, interaction_participant ip, interaction i, interaction_type it, role r "
			 * ; aSql += "WHERE i.id in (" + idList + ") "; aSql +=
			 * "AND pe.id=ip.participant_id "; aSql +=
			 * "AND ip.interaction_id=i.id "; aSql +=
			 * "AND i.interaction_type=it.id "; aSql +=
			 * "AND ip.role_id = r.id "; aSql +=
			 * "order by it.short_name, pe.gene_symbol";
			 */

			DBCollection collection = db.getCollection("interactions_denorm");
			BasicDBObject fields = new BasicDBObject();
			fields.put("primary_accession", 1);
			fields.put("secondary_accession", 1);
			fields.put("gene_symbol", 1);
			fields.put("role_name", 1);
			fields.put("interaction_type", 1);
			fields.put("_id", 0);
			DBObject project = new BasicDBObject("$project", fields);

			boolean hasData = false;

			DBObject inQuery = new BasicDBObject();
			inQuery.put("interaction_id", new BasicDBObject("$in", idList));
			DBObject match = new BasicDBObject("$match", inQuery);
			DBObject sortFields = new BasicDBObject("interaction_type", 1);
			sortFields.put("gene_symbol", 1);
			DBObject sort = new BasicDBObject("$sort", sortFields);
			List<DBObject> pipeline = Arrays.asList(match, project, sort);
			AggregationOutput output = collection.aggregate(pipeline);
			logger.info("End get ...." + tfGene.gene);

			String targetGene = null;
			String shortName = null;
			String previousShortName = null;
			String roleName = null;

			for (DBObject result : output.results()) {
				if (presentBy.equalsIgnoreCase(GeneSymbolOnly)) {
					targetGene = result.get("gene_symbol").toString();
				} else if (presentBy.equalsIgnoreCase(GeneSymbolPreferred)) {
					targetGene = result.get("gene_symbol").toString();
					if (targetGene == null || targetGene.trim().equals("")
							|| targetGene.trim().equalsIgnoreCase("UNKNOWN"))
						targetGene = result.get("primary_accession").toString();
					if (targetGene == null || targetGene.trim().equals("")
							|| targetGene.trim().equalsIgnoreCase("UNKNOWN"))
						targetGene = result.get("secondary_accession")
								.toString();
				} else if (presentBy.equalsIgnoreCase(EntrezIDOnly))
					targetGene = result.get("primary_accession").toString();
				else if (presentBy.equalsIgnoreCase(EntrezIDPreferred)) {
					targetGene = result.get("primary_accession").toString();
					if (targetGene == null || targetGene.trim().equals("")
							|| targetGene.trim().equalsIgnoreCase("UNKNOWN"))
						targetGene = result.get("secondary_accession")
								.toString();
				}

				shortName = interactionTypeMap.get(result.get("interaction_type").toString());
				roleName = result.get("name").toString();

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

		}

		logger.info("End exporting....");
	}

	public void getInteractionsAdjFormat(int versionId, String interactionType,
			String presentBy, PrintWriter out, DB db) throws MongoException {

		logger.info("Start exporting....");

		if (presentBy.equalsIgnoreCase(GENE_NAME))
			presentBy = GeneSymbolOnly;
		else if (presentBy.equalsIgnoreCase(GENE_ID))
			presentBy = EntrezIDPreferred;

		List<GeneInfo> tfList = getRelatedInteractionGenes(versionId,
				interactionType, presentBy, db);

		out.println("adj format data");

		for (GeneInfo tfGene : tfList) {

			if (tfGene.gene == null || tfGene.gene.trim().equals("")
					|| tfGene.gene.equalsIgnoreCase("UNKNOWN")) {
				logger.info("drop gene ...." + tfGene);
				continue;
			}
			logger.debug("Start get ...." + tfGene.gene);
			List<Integer> idList = getInteractionIds(tfGene, interactionType,
					versionId, db);
			logger.debug("get idlist");
			/*
			 * aSql =
			 * "SELECT pe.primary_accession, pe.secondary_accession, pe.gene_symbol, r.name, ic.score "
			 * ; aSql +=
			 * "FROM (physical_entity pe, interaction_participant ip, interaction i,role r) "
			 * ; aSql +=
			 * "left join interaction_confidence ic on (i.id=ic.interaction_id) "
			 * ; aSql += "WHERE i.id in (" + idList + ") "; aSql +=
			 * "AND pe.id=ip.participant_id "; aSql +=
			 * "AND ip.interaction_id=i.id "; aSql += "AND ip.role_id = r.id ";
			 * aSql += "order by pe.gene_symbol";
			 */

			DBCollection collection = db.getCollection("interactions_denorm");
			BasicDBObject fields = new BasicDBObject();
			fields.put("primary_accession", 1);
			fields.put("secondary_accession", 1);
			fields.put("gene_symbol", 1);
			fields.put("role_name", 1);
			fields.put("confidence_value", 1);
			fields.put("_id", 0);
			DBObject project = new BasicDBObject("$project", fields);
			DBObject inQuery = new BasicDBObject();
			inQuery.put("interaction_id", new BasicDBObject("$in", idList));
			DBObject match = new BasicDBObject("$match", inQuery);

			DBObject sort = new BasicDBObject("$sort", new BasicDBObject(
					"gene_symbol", 1));
			List<DBObject> pipeline = Arrays.asList(match, project, sort);
			AggregationOutput output = collection.aggregate(pipeline);

			String targetGene = null;
			String confidence = null;
			String roleName = null;
			boolean hasData = false;

			for (DBObject result : output.results()) {

				if (presentBy.equalsIgnoreCase(GeneSymbolOnly)) {
					targetGene = result.get("gene_symbol").toString();
				} else if (presentBy.equalsIgnoreCase(GeneSymbolPreferred)) {
					targetGene = result.get("gene_symbol").toString();
					if (targetGene == null || targetGene.trim().equals("")
							|| targetGene.equalsIgnoreCase("UNKNOWN"))
						targetGene = result.get("primary_accession").toString();
					if (targetGene == null || targetGene.trim().equals("")
							|| targetGene.equalsIgnoreCase("UNKNOWN"))
						targetGene = result.get("secondary_accession")
								.toString();
				} else if (presentBy.equalsIgnoreCase(EntrezIDOnly))
					targetGene = result.get("primary_accession").toString();
				else if (presentBy.equalsIgnoreCase(EntrezIDPreferred)) {
					targetGene = result.get("primary_accession").toString();
					if (targetGene == null || targetGene.trim().equals("")
							|| targetGene.equalsIgnoreCase("UNKNOWN"))
						targetGene = result.get("secondary_accession")
								.toString();
				}

				roleName = result.get("name").toString();

				if (targetGene == null || targetGene.trim().equals("")
						|| targetGene.trim().equalsIgnoreCase("UNKNOWN")
						|| targetGene.equals(tfGene.gene)
						|| roleName.equals(MODULATOR))
					continue;

				confidence = result.get("score").toString();
				if (hasData == false) {
					out.print(tfGene.gene + "\t" + targetGene + "\t"
							+ confidence);
					hasData = true;
				} else
					out.print("\t" + targetGene + "\t" + confidence);

			}
			if (hasData)
				out.println();

		}

		logger.info("End exporting....");
	}

	private List<GeneInfo> getRelatedInteractionGenes(int versionId,
			String interactionType, String presentBy, DB db)
			throws MongoException {

		if (presentBy.equalsIgnoreCase(EntrezIDPreferred))
			return getRelatedGenesByEntrezIdPref(versionId, interactionType, db);
		else if (presentBy.equalsIgnoreCase(GeneSymbolPreferred))
			return getRelatedGenesBySymbolPref(versionId, interactionType, db);
		else if (presentBy.equalsIgnoreCase(EntrezIDOnly))
			return getRelatedGenesByEntrezIdOnly(versionId, interactionType, db);
		else
			return getRelatedGenesBySymbolOnly(versionId, interactionType, db);

	}

	private List<GeneInfo> getRelatedGenesBySymbolOnly(int versionId,
			String interactionType, DB db) throws MongoException {

		List<GeneInfo> tfList = new ArrayList<GeneInfo>();

		/*
		 * aSql = "SELECT pe.gene_symbol, it.short_name "; aSql +=
		 * "FROM interaction_interactome_version iiv, physical_entity pe, interaction_participant ip, interaction i, interaction_type it, role r "
		 * ; aSql += "WHERE pe.id=ip.participant_id "; aSql +=
		 * "AND ip.interaction_id=i.id "; aSql +=
		 * "AND i.id = iiv.interaction_id "; aSql += "AND r.name <> '" + TARGET
		 * + "' "; aSql += "AND ip.role_id = r.id "; aSql +=
		 * "And iiv.interactome_version_id = " + versionId + " "; if
		 * (!interactionType.equalsIgnoreCase("ALL")) { aSql +=
		 * "AND it.id = i.interaction_type "; aSql += "and it.name ='" +
		 * interactionType + "' "; } aSql += "group by pe.gene_symbol";
		 */

		DBCollection collection = db.getCollection("interactions_denorm");

		BasicDBObject whereQuery = new BasicDBObject();
		whereQuery.put("interactome_version_id", versionId);
		if (!interactionType.equalsIgnoreCase("ALL"))
			whereQuery.put("interaction_type", interactionType);
		whereQuery.put("role_name", new BasicDBObject("$ne", TARGET));
		DBObject match = new BasicDBObject("$match", whereQuery);
		BasicDBObject fields = new BasicDBObject();
		fields.put("gene_symbol", 1);
		fields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", fields);
		DBObject groupIdFields = new BasicDBObject( "_id", "$gene_symbol");
		DBObject groupObject = new BasicDBObject("$addToSet",
				"$gene_symbol");
		groupIdFields.put("gene_symbol", groupObject);
		DBObject group = new BasicDBObject("$group", groupIdFields);
		List<DBObject> pipeline = Arrays.asList(match, project, group);
		AggregationOutput output = collection.aggregate(pipeline);

		for (DBObject result : output.results()) {
			String gene = result.get("gene_symbol").toString();
			tfList.add(new GeneInfo(gene));
		}

		return tfList;
	}

	private List<GeneInfo> getRelatedGenesBySymbolPref(int versionId,
			String interactionType, DB db) throws MongoException {

		List<GeneInfo> tfList = null;

		tfList = getRelatedGenesBySymbolOnly(versionId, interactionType, db);

		/*
		 * aSql =
		 * "SELECT pe.primary_accession, pe.secondary_accession, it.short_name "
		 * ; aSql +=
		 * "FROM interaction_interactome_version iiv, physical_entity pe, interaction_participant ip, interaction i, interaction_type it, role r "
		 * ; aSql += "WHERE " +
		 * "(pe.gene_symbol is null OR pe.gene_symbol='UNKNOWN') "; aSql +=
		 * "AND pe.id=ip.participant_id "; aSql +=
		 * "AND ip.interaction_id=i.id "; aSql +=
		 * "AND i.id = iiv.interaction_id "; aSql += "AND r.name <> '" + TARGET
		 * + "' "; aSql += "AND ip.role_id = r.id "; aSql +=
		 * "And iiv.interactome_version_id = " + versionId + " "; if
		 * (!interactionType.equalsIgnoreCase("ALL")) { aSql +=
		 * "AND it.id = i.interaction_type "; aSql += "and it.name ='" +
		 * interactionType + "' "; } aSql +=
		 * "group by pe.primary_accession, pe.secondary_accession";
		 */

		DBCollection collection = db.getCollection("interactions_denorm");
		BasicDBObject whereQuery = new BasicDBObject();
		whereQuery.put("interactome_version_id", versionId);
		whereQuery.put("gene_symbol", new BasicDBObject("$exists", "false"));
		if (!interactionType.equalsIgnoreCase("ALL"))
			whereQuery.put("interaction_type", interactionType);
		whereQuery.put("role_name", new BasicDBObject("$not", TARGET));
		DBObject match = new BasicDBObject("$match", whereQuery);
		BasicDBObject fields = new BasicDBObject();
		fields.put("primary_accession", 1);
		fields.put("secondary_accession", 1);
		fields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", fields);
		DBObject groupIdFields = new BasicDBObject("primary_accession",
				"$primary_accession");
		groupIdFields.put("secondary_accession", "$secondary_accession");
		DBObject group = new BasicDBObject("$group", groupIdFields);
		List<DBObject> pipeline = Arrays.asList(match, project, group);
		AggregationOutput output = collection.aggregate(pipeline);

		if (tfList == null)
			tfList = new ArrayList<GeneInfo>();

		for (DBObject result : output.results()) {
			String gene = result.get("primary_accession").toString();
			if (gene == null || gene.trim().equals("")
					|| gene.trim().equals("null"))
				tfList.add(new GeneInfo(result.get("secondary_accession")
						.toString(), false));
			else
				tfList.add(new GeneInfo(gene, true));
		}

		return tfList;
	}

	private List<GeneInfo> getRelatedGenesByEntrezIdOnly(int versionId,
			String interactionType, DB db) throws MongoException {

		List<GeneInfo> tfList = new ArrayList<GeneInfo>();

		/*
		 * aSql = "SELECT pe.primary_accession,it.short_name "; aSql +=
		 * "FROM interaction_interactome_version iiv, physical_entity pe, interaction_participant ip, interaction i, interaction_type it, role r "
		 * ; aSql += "WHERE pe.primary_accession is not null "; aSql +=
		 * "AND pe.id=ip.participant_id "; aSql +=
		 * "AND ip.interaction_id=i.id "; aSql +=
		 * "AND i.id = iiv.interaction_id "; aSql += "AND r.name <> '" + TARGET
		 * + "' "; aSql += "AND ip.role_id = r.id "; aSql +=
		 * "And iiv.interactome_version_id = " + versionId + " "; if
		 * (!interactionType.equalsIgnoreCase("ALL")) { aSql +=
		 * "AND it.id = i.interaction_type "; aSql += "and it.name ='" +
		 * interactionType + "' "; }
		 * 
		 * aSql += "group by pe.primary_accession ";
		 */

		DBCollection collection = db.getCollection("interactions_denorm");

		BasicDBObject whereQuery = new BasicDBObject();
		whereQuery.put("interactome_version_id", versionId);
		if (!interactionType.equalsIgnoreCase("ALL"))
			whereQuery.put("interaction_type", interactionType);
		whereQuery.put("role_name", new BasicDBObject("$not", TARGET));
		DBObject match = new BasicDBObject("$match", whereQuery);
		BasicDBObject fields = new BasicDBObject();
		fields.put("primary_accession", 1);
		fields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", fields);
		DBObject groupIdFields = new BasicDBObject("primary_accession",
				"$primary_accession");
		DBObject group = new BasicDBObject("$group", groupIdFields);
		List<DBObject> pipeline = Arrays.asList(match, project, group);
		AggregationOutput output = collection.aggregate(pipeline);

		for (DBObject result : output.results()) {
			String gene = result.get("primary_accession").toString();
			tfList.add(new GeneInfo(gene, true));
		}

		return tfList;
	}

	private List<GeneInfo> getRelatedGenesByEntrezIdPref(int versionId,
			String interactionType, DB db) throws MongoException {

		List<GeneInfo> tfList = null;

		tfList = getRelatedGenesByEntrezIdOnly(versionId, interactionType, db);

		/*
		 * aSql = "SELECT pe.secondary_accession, it.short_name "; aSql +=
		 * "FROM interaction_interactome_version iiv, physical_entity pe, interaction_participant ip, interaction i, interaction_type it, role r "
		 * ; aSql += "WHERE  pe.secondary_accession is not null "; aSql +=
		 * "AND pe.id=ip.participant_id "; aSql +=
		 * "AND ip.interaction_id=i.id "; aSql +=
		 * "AND i.id = iiv.interaction_id "; aSql += "AND r.name <> '" + TARGET
		 * + "' "; aSql += "AND ip.role_id = r.id "; aSql +=
		 * "And iiv.interactome_version_id = " + versionId + " "; if
		 * (!interactionType.equalsIgnoreCase("ALL")) { aSql +=
		 * "AND it.id = i.interaction_type "; aSql += "and it.name ='" +
		 * interactionType + "' "; } aSql += "group by pe.secondary_accession";
		 */

		DBCollection collection = db.getCollection("interactions_denorm");

		BasicDBObject whereQuery = new BasicDBObject();
		whereQuery.put("interactome_version_id", versionId);
		whereQuery.put("secondary_accession", new BasicDBObject("$exists",
				"true"));
		if (!interactionType.equalsIgnoreCase("ALL"))
			whereQuery.put("interaction_type", interactionType);
		whereQuery.put("role_name", new BasicDBObject("$not", TARGET));
		DBObject match = new BasicDBObject("$match", whereQuery);
		BasicDBObject fields = new BasicDBObject();
		fields.put("secondary_accession", 1);
		fields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", fields);
		DBObject groupIdFields = new BasicDBObject("secondary_accession",
				"$secondary_accession");
		DBObject group = new BasicDBObject("$group", groupIdFields);
		List<DBObject> pipeline = Arrays.asList(match, project, group);
		AggregationOutput output = collection.aggregate(pipeline);

		if (tfList == null)
			tfList = new ArrayList<GeneInfo>();

		for (DBObject result : output.results()) {
			String gene = result.get("secondary_accession").toString();
			tfList.add(new GeneInfo(gene, false));
		}

		return tfList;
	}

	private List<Integer> getInteractionIds(GeneInfo tfGene,
			String interactionType, int versionId, DB db) throws MongoException {

		/*
		 * aSql = "SELECT i.id "; aSql +=
		 * "FROM interaction_interactome_version iiv, physical_entity pe, interaction_participant ip, interaction i, interaction_type it "
		 * ; aSql += "WHERE pe." + colName + " = ? "; aSql +=
		 * "AND iiv.interactome_version_id = ? "; if
		 * (!interactionType.equalsIgnoreCase("ALL")) { aSql += "and it.name ='"
		 * + interactionType + "' "; aSql += "AND it.id = i.interaction_type ";
		 * } aSql += "AND ip.interaction_id=i.id "; aSql +=
		 * "AND i.id = iiv.interaction_id "; aSql +=
		 * "AND pe.id=ip.participant_id  ";
		 */

		DBCollection collection = db.getCollection("interactions_denorm");

		BasicDBObject whereQuery = new BasicDBObject();
		whereQuery.put("interactome_version_id", versionId);

		if (tfGene.isPrimary == null)
			whereQuery.put("gene_symbol", tfGene.gene);
		else if (tfGene.isPrimary.booleanValue() == true)
			whereQuery.put("primary_accession", tfGene.gene);
		else
			whereQuery.put("secondary_accession", tfGene.gene);
		if (!interactionType.equalsIgnoreCase("ALL"))
			whereQuery.put("interaction_type", interactionType);
		DBObject match = new BasicDBObject("$match", whereQuery);
		BasicDBObject fields = new BasicDBObject();
		fields.put("interaction_id", 1);
		fields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", fields);
		List<DBObject> pipeline = Arrays.asList(match, project);
		AggregationOutput output = collection.aggregate(pipeline);

		List<Integer> idList = new ArrayList<Integer>();
		for (DBObject result : output.results()) {
			Integer id = new Integer(result.get("interaction_id").toString());
			idList.add(id);

		}

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
	
	private HashMap<String, String> getInteractionTypeMap(DB db) throws MongoException {

		HashMap<String, String> interactionTypeMap = new HashMap<String, String>();
		DBCollection collection = db.getCollection("interaction_type");
		DBObject fields = new BasicDBObject();
		fields.put("interaction_type", "$name");
		fields.put("short_name", 1);
		fields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", fields);
		List<DBObject> pipeline = Arrays.asList(project);
		AggregationOutput output = collection.aggregate(pipeline);
		for (DBObject result : output.results()) {
			String interactionType = result.get("interaction_type").toString().trim();
			String short_name = result.get("short_name").toString().trim();
			interactionTypeMap.put(interactionType, short_name);
		}
		
		return interactionTypeMap;
	}

}
