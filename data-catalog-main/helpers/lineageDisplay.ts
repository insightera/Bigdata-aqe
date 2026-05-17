/**
 * Label & layout lineage — selaras diagram Bigdata-pipeline-Metadata.jpg
 * (Staging → Bronze → Silver → Gold + metadata ingestion/enrichment per layer).
 */

import { layerFromQualifiedName } from './atlasApi';

export interface PipelineEdge {
	id: string;
	from: string;
	to: string;
	/** Label di panah (mis. Extract, Transform) */
	stepLabel: string;
	/** Nama pipeline Atlas */
	pipelineName: string;
	/** Jenis metadata lineage di diagram */
	lineageKind: string;
}

/** Urutan layer data + panah ETL di antara layer (bukan satu blok "ETL" generik). */
export const MEDALLION_PIPELINE_EDGES: PipelineEdge[] = [
	{
		id: 'staging_to_bronze',
		from: 'staging',
		to: 'bronze',
		stepLabel: 'Columnar ingest',
		pipelineName: 'staging_to_bronze',
		lineageKind: 'Raw lineage (ingestion)',
	},
	{
		id: 'bronze_to_silver',
		from: 'bronze',
		to: 'silver',
		stepLabel: 'Extract',
		pipelineName: 'bronze_to_silver',
		lineageKind: 'Transformation lineage',
	},
	{
		id: 'silver_to_gold',
		from: 'silver',
		to: 'gold',
		stepLabel: 'Transform & load',
		pipelineName: 'silver_to_gold',
		lineageKind: 'Advanced lineage',
	},
];

export const MEDALLION_DATA_LAYERS = ['staging', 'bronze', 'silver', 'gold'] as const;

/** Metadata yang dihasilkan per layer (sesuai diagram). */
export const LINEAGE_METADATA_BY_LAYER: Record<string, string[]> = {
	staging: ['Source landing', 'File format metadata'],
	bronze: [
		'Raw technical metadata',
		'Raw lineage',
		'Raw data profiling',
		'Raw classification',
	],
	silver: [
		'Clean metadata',
		'Quality metadata',
		'Transformation lineage',
		'Business metadata',
		'Compliance metadata',
	],
	gold: [
		'Business metadata',
		'KPI metadata',
		'AI metadata',
		'Consumption metadata',
		'Advanced lineage',
	],
};

const PIPELINE_LABELS: Record<string, string> = {
	staging_to_bronze: 'Staging → Bronze',
	bronze_to_silver: 'Bronze → Silver',
	silver_to_gold: 'Silver → Gold',
};

const PIPELINE_SLOT: Record<string, string> = {
	staging_to_bronze: 'etl-staging-bronze',
	bronze_to_silver: 'etl-bronze-silver',
	silver_to_gold: 'etl-silver-gold',
};

export function resolveEntityLayer(
	qualifiedName: string,
	typeName: string,
	attrs: Record<string, any> = {},
): string {
	if (typeName === 'lakehouse_etl_process') {
		const pipeline = String(attrs.pipeline_name || '');
		if (PIPELINE_SLOT[pipeline]) return PIPELINE_SLOT[pipeline];

		const src = attrs.source_layer;
		const tgt = attrs.target_layer;
		if (src && tgt) return `etl-${src}-${tgt}`;

		if (qualifiedName.startsWith('etl.')) {
			const segment = qualifiedName.split('.')[1] || '';
			if (segment.includes('staging_to_bronze')) return 'etl-staging-bronze';
			if (segment.includes('bronze_to_silver')) return 'etl-bronze-silver';
			if (segment.includes('silver_to_gold')) return 'etl-silver-gold';
		}
		return 'etl';
	}

	const fromQn = layerFromQualifiedName(qualifiedName);
	if (fromQn !== 'unknown') return fromQn;
	if (attrs.layer) return String(attrs.layer);
	return 'unknown';
}

export function resolveDisplayName(
	name: string,
	typeName: string,
	attrs: Record<string, any> = {},
): string {
	if (typeName === 'lakehouse_etl_process') {
		const pipeline = String(attrs.pipeline_name || '');
		if (PIPELINE_LABELS[pipeline]) {
			return `ETL ${PIPELINE_LABELS[pipeline]}`;
		}
		if (attrs.name) return String(attrs.name).replace(/_/g, ' ');
		return 'ETL process';
	}
	return name || attrs.name || attrs.qualifiedName || 'Dataset';
}

export function layerBadgeLabel(layerKey: string): string {
	const labels: Record<string, string> = {
		staging: 'Staging',
		bronze: 'Bronze',
		silver: 'Silver',
		gold: 'Gold',
		'etl-staging-bronze': 'ETL · Staging→Bronze',
		'etl-bronze-silver': 'ETL · Bronze→Silver',
		'etl-silver-gold': 'ETL · Silver→Gold',
		etl: 'ETL',
		process: 'ETL',
		unknown: 'Metadata',
	};
	return labels[layerKey] || layerKey;
}

export function processMatchesEdge(
	attrs: Record<string, any>,
	edge: PipelineEdge,
): boolean {
	const pipeline = String(attrs.pipeline_name || '');
	if (pipeline === edge.pipelineName) return true;
	return attrs.source_layer === edge.from && attrs.target_layer === edge.to;
}

export function parseLineageEntity(
	guid: string,
	entity: { typeName: string; attributes?: Record<string, any> },
) {
	const attrs = entity.attributes || {};
	const qualifiedName = String(attrs.qualifiedName || '');
	const layer = resolveEntityLayer(qualifiedName, entity.typeName, attrs);
	return {
		guid,
		name: attrs.name || qualifiedName || guid,
		displayName: resolveDisplayName(
			String(attrs.name || ''),
			entity.typeName,
			attrs,
		),
		typeName: entity.typeName,
		qualifiedName,
		layer,
		layerLabel: layerBadgeLabel(layer),
		pipelineName: attrs.pipeline_name as string | undefined,
		isProcess: entity.typeName === 'lakehouse_etl_process',
		attributes: attrs,
	};
}

export type ParsedLineageNode = ReturnType<typeof parseLineageEntity>;
