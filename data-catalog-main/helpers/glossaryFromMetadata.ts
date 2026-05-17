import type { AtlasEntity } from './atlasApi';
import { layerFromQualifiedName, searchEntities } from './atlasApi';
import { parseEntityProfiling, getBusinessMeta } from './entityProfiling';

export interface GlossaryTermFromMetadata {
	term: string;
	definition: string;
	category: string;
	source: 'silver_metadata' | 'gold_metadata' | 'local';
	related_assets?: string[];
}

/** Definisi untuk istilah yang muncul di BUSINESS_METADATA Silver */
const TERM_DEFINITIONS: Record<string, { definition: string; category: string }> = {
	'Mahasiswa Aktif': {
		definition: 'Mahasiswa dengan status akademik aktif pada semester berjalan',
		category: 'Academic',
	},
	MBKM: {
		definition: 'Merdeka Belajar Kampus Merdeka — program kredit di luar kampus (≥20 SKS)',
		category: 'Academic',
	},
	'SKS Luar Kampus': {
		definition: 'Satuan kredit semester yang diperoleh dari kegiatan di luar perguruan tinggi',
		category: 'Academic',
	},
	'Lulusan Terserap': {
		definition: 'Lulusan yang bekerja, melanjutkan studi, atau berwirausaha (tracer study)',
		category: 'Business',
	},
	'Tracer Study': {
		definition: 'Survei pelacakan alumni untuk mengukur penyerapan lulusan',
		category: 'Business',
	},
	'Masa Tunggu': {
		definition: 'Waktu antara lulus dan memperoleh pekerjaan pertama',
		category: 'Business',
	},
	'Dosen Tetap': {
		definition: 'Dosen dengan status kepegawaian tetap di perguruan tinggi',
		category: 'Academic',
	},
	Tridarma: {
		definition: 'Tri Dharma Perguruan Tinggi: pendidikan, penelitian, pengabdian masyarakat',
		category: 'Academic',
	},
	Serdos: {
		definition: 'Sertifikasi Dosen — sertifikat kompetensi mengajar',
		category: 'Academic',
	},
	'Jabatan Fungsional': {
		definition: 'Jenjang kepangkatan akademik dosen (Asisten Ahli, Lektor, dst.)',
		category: 'Academic',
	},
	'Rekognisi Internasional': {
		definition: 'Publikasi atau output penelitian terindeks internasional (Scopus/WoS)',
		category: 'Academic',
	},
	'Pengabdian Masyarakat': {
		definition: 'Kegiatan dosen untuk memecahkan masalah masyarakat (PkM)',
		category: 'Academic',
	},
	'Hibah Penelitian': {
		definition: 'Pendanaan eksternal untuk kegiatan penelitian',
		category: 'Academic',
	},
	MoU: {
		definition: 'Memorandum of Understanding — perjanjian kerjasama institusi',
		category: 'Governance',
	},
	PKS: {
		definition: 'Perjanjian Kerja Sama operasional antara ITERA dan mitra',
		category: 'Governance',
	},
	'Mitra Kerjasama': {
		definition: 'Organisasi/industri mitra dalam kerjasama pendidikan atau riset',
		category: 'Governance',
	},
	'BAN-PT': {
		definition: 'Badan Akreditasi Nasional Perguruan Tinggi',
		category: 'Governance',
	},
	LAM: {
		definition: 'Lembaga Akreditasi Mandiri untuk program studi',
		category: 'Governance',
	},
	'Akreditasi Internasional': {
		definition: 'Akreditasi prodi oleh lembaga internasional (AACSB, ABET, dll.)',
		category: 'Governance',
	},
	Unggul: {
		definition: 'Peringkat akreditasi tertinggi dalam sistem BAN-PT',
		category: 'Governance',
	},
	IKU: {
		definition: 'Indikator Kinerja Utama — metrik kinerja institusi dari Renstra',
		category: 'Business',
	},
	SAKIP: {
		definition: 'Sistem Akuntabilitas Kinerja Instansi Pemerintah',
		category: 'Governance',
	},
	PII: {
		definition: 'Personally Identifiable Information — data yang dapat mengidentifikasi individu',
		category: 'Security',
	},
};

const FALLBACK_TERMS: GlossaryTermFromMetadata[] = [
	{
		term: 'Medallion Architecture',
		definition:
			'Arsitektur lakehouse berlapis: Staging (CSV), Bronze (raw), Silver (clean), Gold (curated)',
		category: 'Technical',
		source: 'local',
	},
	{
		term: 'Star Schema',
		definition: 'Model data warehouse dengan tabel fakta dan dimensi untuk OLAP',
		category: 'Technical',
		source: 'local',
	},
	{
		term: 'Data Lineage',
		definition: 'Jejak transformasi data dari sumber hingga konsumsi',
		category: 'Governance',
		source: 'local',
	},
	{
		term: 'Apache Iceberg',
		definition: 'Format tabel open-source dengan ACID dan time travel di data lake',
		category: 'Technical',
		source: 'local',
	},
	{
		term: 'Apache Atlas',
		definition: 'Platform metadata dan data governance',
		category: 'Technical',
		source: 'local',
	},
	{
		term: 'Data Quality Score',
		definition: 'Skor kelengkapan data: PASS ≥80%, QUARANTINE 60–79%, REJECT <60%',
		category: 'Quality',
		source: 'local',
	},
	{
		term: 'Renstra',
		definition: 'Rencana Strategis ITERA 2020–2024 sebagai acuan target IKU',
		category: 'Business',
		source: 'local',
	},
];

function definitionForTerm(term: string): { definition: string; category: string } {
	if (TERM_DEFINITIONS[term]) return TERM_DEFINITIONS[term];
	return {
		definition: `Istilah bisnis terkait metadata lakehouse ITERA: ${term}`,
		category: 'Business',
	};
}

export function collectGlossaryTermsFromEntities(entities: AtlasEntity[]): GlossaryTermFromMetadata[] {
	const byTerm = new Map<string, GlossaryTermFromMetadata>();

	for (const entity of entities) {
		const attrs = entity.attributes || {};
		const qn = String(attrs.qualifiedName || '');
		const layer = String(attrs.layer || layerFromQualifiedName(qn));
		const profiling = parseEntityProfiling(attrs.profiling);
		const biz = getBusinessMeta(profiling);

		if (biz?.glossary_terms) {
			for (const term of biz.glossary_terms) {
				const def = definitionForTerm(term);
				const existing = byTerm.get(term);
				if (existing) {
					if (!existing.related_assets?.includes(qn)) {
						existing.related_assets = [...(existing.related_assets || []), qn];
					}
				} else {
					byTerm.set(term, {
						term,
						definition: def.definition,
						category: def.category,
						source: 'silver_metadata',
						related_assets: [qn],
					});
				}
			}
		}

		const kpi = profiling.kpi as Record<string, unknown> | undefined;
		if (layer === 'gold' && kpi?.iku_code) {
			const code = String(kpi.iku_code);
			const nama = String(kpi.iku_nama || code);
			byTerm.set(code, {
				term: code,
				definition: nama,
				category: 'Business',
				source: 'gold_metadata',
				related_assets: [qn],
			});
		}
	}

	return [...byTerm.values()].sort((a, b) => a.term.localeCompare(b.term));
}

export async function buildGlossaryFromAtlas(limit = 500): Promise<{
	terms: GlossaryTermFromMetadata[];
	fallback: GlossaryTermFromMetadata[];
	silverEntityCount: number;
	generatedAt: string;
}> {
	const result = await searchEntities('lakehouse_dataset', undefined, undefined, limit, 0);
	const entities = result.entities || [];
	const silverCount = entities.filter(
		(e) => (e.attributes?.layer || layerFromQualifiedName(e.attributes?.qualifiedName)) === 'silver',
	).length;

	return {
		terms: collectGlossaryTermsFromEntities(entities),
		fallback: FALLBACK_TERMS,
		silverEntityCount: silverCount,
		generatedAt: new Date().toISOString(),
	};
}

export function mergeGlossaryTerms(
	atlasTerms: { term: string; definition: string; category: string; guid?: string }[],
	metadataTerms: GlossaryTermFromMetadata[],
	fallbackTerms: GlossaryTermFromMetadata[],
): Array<GlossaryTermFromMetadata & { source: string; guid?: string }> {
	const map = new Map<string, GlossaryTermFromMetadata & { source: string; guid?: string }>();

	for (const t of fallbackTerms) {
		map.set(t.term.toLowerCase(), { ...t, source: 'local' });
	}
	for (const t of metadataTerms) {
		map.set(t.term.toLowerCase(), { ...t, source: t.source });
	}
	for (const t of atlasTerms) {
		map.set(t.term.toLowerCase(), {
			term: t.term,
			definition: t.definition,
			category: t.category,
			source: 'atlas',
			guid: t.guid,
		});
	}

	return [...map.values()].sort((a, b) => a.term.localeCompare(b.term));
}
