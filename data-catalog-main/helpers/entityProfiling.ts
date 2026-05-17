/** Parse Atlas entity.attributes.profiling JSON across Bronze / Silver / Gold shapes. */

export interface ColumnStat {
	data_type?: string;
	null_count?: number;
	null_pct?: number;
	distinct_count?: number;
	completeness_pct?: number;
}

export interface QualityMeta {
	quality_score?: number;
	status?: string;
	source_status?: string;
	source_score?: number;
	silver_completeness?: number;
	overall_score?: number;
	avg_completeness?: number;
	rules_passed?: number;
	rules_total?: number;
}

export interface BusinessMeta {
	owner?: string;
	iku_relevance?: string[];
	glossary_terms?: string[];
	update_frequency?: string;
	domain?: string;
}

export interface ComplianceMeta {
	contains_pii?: boolean;
	pii_columns?: string[];
	data_classification?: string;
	retention_policy?: string;
	access_control?: string;
}

export function parseEntityProfiling(raw: unknown): Record<string, unknown> {
	if (!raw) return {};
	if (typeof raw === 'object') return raw as Record<string, unknown>;
	if (typeof raw !== 'string') return {};
	try {
		const parsed = JSON.parse(raw);
		return typeof parsed === 'object' && parsed !== null ? parsed : {};
	} catch {
		return {};
	}
}

function isColumnStat(v: unknown): v is ColumnStat {
	if (!v || typeof v !== 'object') return false;
	const o = v as ColumnStat;
	return (
		o.null_pct !== undefined ||
		o.completeness_pct !== undefined ||
		o.null_count !== undefined
	);
}

/** Bronze stores column stats at profiling root; Silver under profiling.columns */
export function getColumnProfiling(profiling: Record<string, unknown>): Record<string, ColumnStat> {
	const nested = profiling.columns;
	if (nested && typeof nested === 'object' && !Array.isArray(nested)) {
		return nested as Record<string, ColumnStat>;
	}
	const keys = Object.keys(profiling);
	if (keys.length > 0 && isColumnStat(profiling[keys[0]])) {
		return profiling as Record<string, ColumnStat>;
	}
	return {};
}

export function getQualityMeta(profiling: Record<string, unknown>): QualityMeta | null {
	const q = profiling.quality;
	if (!q || typeof q !== 'object') return null;
	const meta = q as QualityMeta;
	if (
		meta.quality_score == null &&
		meta.source_score == null &&
		!meta.source_status &&
		!meta.status
	) {
		return null;
	}
	return meta;
}

export function qualityDisplayScore(q: QualityMeta): number | string {
	return (
		q.quality_score ??
		q.source_score ??
		q.overall_score ??
		q.silver_completeness ??
		'—'
	);
}

export function qualityDisplayStatus(q: QualityMeta): string {
	return q.status || q.source_status || 'UNKNOWN';
}

export function getBusinessMeta(profiling: Record<string, unknown>): BusinessMeta | null {
	const b = profiling.business;
	if (!b || typeof b !== 'object') return null;
	const biz = b as BusinessMeta;
	if (
		!biz.owner &&
		!(biz.iku_relevance?.length) &&
		!(biz.glossary_terms?.length) &&
		!biz.update_frequency
	) {
		return null;
	}
	return biz;
}

export function getComplianceMeta(
	profiling: Record<string, unknown>,
	piiColumnsFallback: string[],
): ComplianceMeta | null {
	const c = profiling.compliance;
	const base: ComplianceMeta =
		c && typeof c === 'object' ? { ...(c as ComplianceMeta) } : {};
	if (piiColumnsFallback.length > 0) {
		base.contains_pii = base.contains_pii ?? true;
		base.pii_columns = base.pii_columns?.length
			? base.pii_columns
			: piiColumnsFallback;
	}
	if (
		base.contains_pii === undefined &&
		!base.data_classification &&
		!base.pii_columns?.length
	) {
		return null;
	}
	return base;
}

export function getTransformations(profiling: Record<string, unknown>): string[] {
	const t = profiling.transformations;
	if (!Array.isArray(t)) return [];
	return t.filter((x): x is string => typeof x === 'string' && x.length > 0);
}

export function qualityStatusColor(status?: string): 'success' | 'warning' | 'danger' | 'secondary' {
	switch ((status || '').toUpperCase()) {
		case 'PASS':
			return 'success';
		case 'QUARANTINE':
			return 'warning';
		case 'REJECT':
			return 'danger';
		default:
			return 'secondary';
	}
}
