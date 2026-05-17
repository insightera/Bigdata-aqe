import type { AtlasEntity } from './atlasApi';
import { layerFromQualifiedName, searchEntities } from './atlasApi';

export interface LayerQualityMetrics {
	layer: string;
	label: string;
	entityCount: number;
	completeness: number;
	accuracy: number;
	timeliness: number;
	consistency: number;
	completenessScore15: number;
	accuracyScore15: number;
	timelinessScore15: number;
	consistencyScore15: number;
}

export interface MetadataQualityReport {
	generatedAt: string;
	layers: LayerQualityMetrics[];
	methodology: string;
}

const EVAL_LAYERS = [
	{ layer: 'bronze', label: 'Bronze', classification: 'Bronze_Layer' },
	{ layer: 'silver', label: 'Silver', classification: 'Silver_Layer' },
	{ layer: 'gold', label: 'Gold', classification: 'Gold_Layer' },
] as const;

function parseJson(raw: unknown): Record<string, unknown> {
	if (!raw) return {};
	if (typeof raw === 'object') return raw as Record<string, unknown>;
	if (typeof raw !== 'string') return {};
	try {
		const v = JSON.parse(raw);
		return typeof v === 'object' && v !== null ? v : {};
	} catch {
		return {};
	}
}

function filled(val: unknown): boolean {
	if (val === null || val === undefined) return false;
	if (typeof val === 'number') return !Number.isNaN(val);
	if (typeof val === 'string') {
		const t = val.trim();
		return t.length > 0 && t !== '{}' && t !== '[]' && t !== 'null';
	}
	if (typeof val === 'object') return Object.keys(val as object).length > 0;
	return true;
}

function pctToScore15(pct: number): number {
	return Math.min(5, Math.max(1, Math.round(pct / 20) || 1));
}

function avg(nums: number[]): number {
	if (nums.length === 0) return 0;
	return nums.reduce((a, b) => a + b, 0) / nums.length;
}

function parseTimestamp(attrs: Record<string, unknown>): number | null {
	const raw = attrs.enriched_at || attrs.ingested_at;
	if (!raw) return null;
	const t = Date.parse(String(raw));
	return Number.isNaN(t) ? null : t;
}

function scoreTimeliness(attrs: Record<string, unknown>): number {
	const ts = parseTimestamp(attrs);
	if (!ts) return 0;
	const ageDays = (Date.now() - ts) / (1000 * 60 * 60 * 24);
	if (ageDays <= 7) return 100;
	if (ageDays <= 30) return 90;
	if (ageDays <= 90) return 75;
	if (ageDays <= 180) return 60;
	return 40;
}

function completenessChecks(
	layer: string,
	attrs: Record<string, unknown>,
	profiling: Record<string, unknown>,
): boolean[] {
	const business = (profiling.business as Record<string, unknown>) || {};
	const quality = (profiling.quality as Record<string, unknown>) || {};
	const schema = parseJson(attrs.schema_def);

	const base = [
		filled(attrs.qualifiedName),
		filled(attrs.name),
		filled(attrs.description),
		filled(attrs.layer),
		filled(attrs.format),
		filled(attrs.location),
		Object.keys(schema).length > 0,
		attrs.row_count !== undefined && attrs.row_count !== null,
		filled(attrs.ingested_at) || filled(attrs.enriched_at),
	];

	if (layer === 'bronze') return base;

	if (layer === 'silver') {
		return [
			...base,
			filled(business.owner),
			filled(business.glossary_terms),
			Object.keys(quality).length > 0,
			filled(profiling.compliance),
		];
	}

	// gold
	const star = (profiling.star_schema as Record<string, unknown>) || {};
	const kpi = (profiling.kpi as Record<string, unknown>) || {};
	return [
		...base,
		filled(star.table_type) || filled(kpi.iku_code),
		filled(profiling.consumption),
		filled(attrs.enriched_at),
	];
}

function scoreCompleteness(
	layer: string,
	attrs: Record<string, unknown>,
	profiling: Record<string, unknown>,
): number {
	const checks = completenessChecks(layer, attrs, profiling);
	const passed = checks.filter(Boolean).length;
	return Math.round((passed / checks.length) * 100);
}

function scoreAccuracy(
	layer: string,
	attrs: Record<string, unknown>,
	profiling: Record<string, unknown>,
): number {
	const schema = parseJson(attrs.schema_def);
	const schemaOk = Object.keys(schema).length > 0;
	const rowOk =
		attrs.row_count === undefined ||
		attrs.row_count === null ||
		Number(attrs.row_count) >= 0;

	if (!schemaOk || !rowOk) return 0;

	if (layer === 'bronze') return 95;

	const quality = (profiling.quality as Record<string, unknown>) || {};
	const qScore =
		quality.overall_score ??
		quality.source_score ??
		quality.score ??
		quality.quality_score;
	if (typeof qScore === 'number') return Math.min(100, Math.max(0, Math.round(qScore)));
	if (quality.source_status === 'PASS') return 90;
	if (quality.source_status === 'QUARANTINE') return 70;

	if (layer === 'silver') return 85;

	const star = (profiling.star_schema as Record<string, unknown>) || {};
	const kpi = (profiling.kpi as Record<string, unknown>) || {};
	if (filled(star.table_type) || filled(kpi.iku_code)) return 92;
	return 75;
}

function scoreConsistency(
	layer: string,
	attrs: Record<string, unknown>,
	classifications: { typeName: string }[],
): number {
	const qn = String(attrs.qualifiedName || '');
	const checks: boolean[] = [
		String(attrs.layer || '') === layer,
		layerFromQualifiedName(qn) === layer,
		qn.startsWith(`${layer}.`),
	];

	const expectedTag = `${layer.charAt(0).toUpperCase()}${layer.slice(1)}_Layer`;
	if (layer === 'bronze' || layer === 'silver' || layer === 'gold') {
		checks.push(classifications.some((c) => c.typeName === expectedTag));
	}

	const loc = String(attrs.location || '').toLowerCase();
	checks.push(loc.includes(layer) || loc.includes(`/${layer}/`));

	const passed = checks.filter(Boolean).length;
	return Math.round((passed / checks.length) * 100);
}

function evaluateEntity(layer: string, entity: AtlasEntity) {
	const attrs = entity.attributes || {};
	const profiling = parseJson(attrs.profiling);
	const classifications = entity.classifications || [];

	return {
		completeness: scoreCompleteness(layer, attrs, profiling),
		accuracy: scoreAccuracy(layer, attrs, profiling),
		timeliness: scoreTimeliness(attrs),
		consistency: scoreConsistency(layer, attrs, classifications),
	};
}

function aggregateLayer(
	layer: string,
	label: string,
	entities: AtlasEntity[],
): LayerQualityMetrics {
	if (entities.length === 0) {
		return {
			layer,
			label,
			entityCount: 0,
			completeness: 0,
			accuracy: 0,
			timeliness: 0,
			consistency: 0,
			completenessScore15: 1,
			accuracyScore15: 1,
			timelinessScore15: 1,
			consistencyScore15: 1,
		};
	}

	const scores = entities.map((e) => evaluateEntity(layer, e));
	const completeness = Math.round(avg(scores.map((s) => s.completeness)));
	const accuracy = Math.round(avg(scores.map((s) => s.accuracy)));
	const timeliness = Math.round(avg(scores.map((s) => s.timeliness)));
	const consistency = Math.round(avg(scores.map((s) => s.consistency)));

	return {
		layer,
		label,
		entityCount: entities.length,
		completeness,
		accuracy,
		timeliness,
		consistency,
		completenessScore15: pctToScore15(completeness),
		accuracyScore15: pctToScore15(accuracy),
		timelinessScore15: pctToScore15(timeliness),
		consistencyScore15: pctToScore15(consistency),
	};
}

export async function buildMetadataQualityReport(): Promise<MetadataQualityReport> {
	const layerResults: LayerQualityMetrics[] = [];

	for (const cfg of EVAL_LAYERS) {
		const result = await searchEntities(
			'lakehouse_dataset',
			undefined,
			cfg.classification,
			100,
			0,
		);
		layerResults.push(aggregateLayer(cfg.layer, cfg.label, result.entities || []));
	}

	return {
		generatedAt: new Date().toISOString(),
		layers: layerResults,
		methodology:
			'Skor dihitung dari rata-rata entitas lakehouse_dataset per layer di Atlas: ' +
			'Completeness = kelengkapan atribut wajib; Accuracy = validitas skema/profil kualitas; ' +
			'Timeliness = kesegaran ingested_at/enriched_at; Consistency = keselarasan layer, ' +
			'qualifiedName, lokasi, dan klasifikasi. Skala 1–5 = pembulatan skor % ÷ 20.',
	};
}
