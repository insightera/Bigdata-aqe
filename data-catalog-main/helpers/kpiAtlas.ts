import type { AtlasEntity } from './atlasApi';

export const LEADERSHIP_CONSUMERS = [
	{
		role: 'Rektor',
		desc: 'Ringkasan capaian seluruh IKU institusi',
		icon: 'Person',
	},
	{
		role: 'Wakil Rektor Bidang Akademik & Kemahasiswaan',
		desc: 'IKU-1, IKU-2, IKU-7 (lulusan, MBKM, metode pembelajaran)',
		icon: 'School',
	},
	{
		role: 'Wakil Rektor Bidang Keuangan & Umum',
		desc: 'SAKIP, anggaran, dan tata kelola institusi',
		icon: 'AccountBalance',
	},
	{
		role: 'Kepala Biro Akademik Perencanaan dan Umum',
		desc: 'Perencanaan strategis, rekap IKU, pelaporan Renstra',
		icon: 'Assignment',
	},
	{
		role: 'Kepala Bagian Umum dan Akademik',
		desc: 'Data operasional akademik dan dimensi prodi/mahasiswa',
		icon: 'MenuBook',
	},
	{
		role: 'Dekan Fakultas Sains',
		desc: 'Drill-down capaian IKU per prodi fakultas Sains',
		icon: 'Biotech',
	},
	{
		role: 'Dekan Fakultas Teknologi Infrastruktur dan Kewilayahan',
		desc: 'Drill-down capaian IKU per prodi fakultas FTIK',
		icon: 'Engineering',
	},
	{
		role: 'Dekan Fakultas Teknologi Industri',
		desc: 'Drill-down capaian IKU per prodi fakultas FTI',
		icon: 'PrecisionManufacturing',
	},
] as const;

export function parseProfiling(entity?: AtlasEntity): Record<string, any> {
	if (!entity?.attributes?.profiling) return {};
	try {
		return JSON.parse(entity.attributes.profiling);
	} catch {
		return {};
	}
}

/** Gabungkan hasil search Atlas; deduplikasi by guid. */
export function mergeAtlasEntities(...lists: AtlasEntity[][]): AtlasEntity[] {
	const byGuid = new Map<string, AtlasEntity>();
	for (const list of lists) {
		for (const e of list) {
			if (e?.guid) byGuid.set(e.guid, e);
		}
	}
	return [...byGuid.values()];
}

export function matchIkuEntity(
	entities: AtlasEntity[],
	iku: { code: string; fact: string },
): AtlasEntity | undefined {
	return entities.find((e) => {
		const name = (e.attributes?.name || '') as string;
		const qn = (e.attributes?.qualifiedName || '') as string;
		const layer = (e.attributes?.layer || '') as string;

		if (name === iku.fact) return true;
		if (qn === `gold.${iku.fact}@lakehouse` || qn.includes(iku.fact)) return true;
		if (layer === 'gold' && name.includes(iku.fact.replace('fact_', ''))) return true;

		const prof = parseProfiling(e);
		if (prof.kpi?.iku_code === iku.code) return true;

		return false;
	});
}

export function kpiStatusColor(status?: string): 'success' | 'warning' | 'danger' | 'info' {
	switch (status) {
		case 'Tercapai':
			return 'success';
		case 'On Track':
			return 'warning';
		case 'Tidak Tercapai':
			return 'danger';
		default:
			return 'info';
	}
}
