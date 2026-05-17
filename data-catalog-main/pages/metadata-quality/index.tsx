import React, { useCallback, useEffect, useState } from 'react';
import type { NextPage } from 'next';
import { GetStaticProps } from 'next';
import Head from 'next/head';
import { serverSideTranslations } from 'next-i18next/serverSideTranslations';
import PageWrapper from '../../layout/PageWrapper/PageWrapper';
import SubHeader, { SubHeaderLeft, SubHeaderRight } from '../../layout/SubHeader/SubHeader';
import Page from '../../layout/Page/Page';
import Card, {
	CardBody,
	CardHeader,
	CardLabel,
	CardTitle,
	CardSubTitle,
} from '../../components/bootstrap/Card';
import Icon from '../../components/icon/Icon';
import Badge from '../../components/bootstrap/Badge';
import Button from '../../components/bootstrap/Button';
import Progress from '../../components/bootstrap/Progress';
import Spinner from '../../components/bootstrap/Spinner';
import type {
	LayerQualityMetrics,
	MetadataQualityReport,
} from '../../helpers/metadataQualityEvaluator';

function pctColor(pct: number): 'success' | 'warning' | 'danger' | 'primary' {
	if (pct >= 80) return 'success';
	if (pct >= 60) return 'warning';
	if (pct === 0) return 'danger';
	return 'primary';
}

function MetricCell({ pct, score15 }: { pct: number; score15: number }) {
	return (
		<div>
			<strong>{pct}%</strong>
			<div className='text-muted small'>(skor {score15}/5)</div>
			<Progress value={pct} color={pctColor(pct)} height={4} className='mt-1' />
		</div>
	);
}

const MetadataQualityPage: NextPage = () => {
	const [report, setReport] = useState<MetadataQualityReport | null>(null);
	const [loading, setLoading] = useState(true);
	const [error, setError] = useState<string | null>(null);

	const fetchReport = useCallback(async () => {
		setLoading(true);
		setError(null);
		try {
			const res = await fetch('/api/atlas/metadata-quality');
			const data = await res.json();
			if (!res.ok) throw new Error(data.error || 'Failed to load report');
			setReport(data);
		} catch (e: unknown) {
			setError(e instanceof Error ? e.message : 'Unknown error');
			setReport(null);
		} finally {
			setLoading(false);
		}
	}, []);

	useEffect(() => {
		fetchReport();
	}, [fetchReport]);

	const exportCsv = () => {
		if (!report) return;
		const header =
			'Layer,Entity Count,Completeness (%),Accuracy (%),Timeliness (%),Consistency (%)';
		const lines = report.layers.map(
			(l) =>
				`${l.label},${l.entityCount},${l.completeness},${l.accuracy},${l.timeliness},${l.consistency}`,
		);
		const blob = new Blob([[header, ...lines].join('\n')], { type: 'text/csv' });
		const url = URL.createObjectURL(blob);
		const a = document.createElement('a');
		a.href = url;
		a.download = `metadata_quality_${new Date().toISOString().slice(0, 10)}.csv`;
		a.click();
		URL.revokeObjectURL(url);
	};

	return (
		<PageWrapper>
			<Head>
				<title>Metadata Quality Evaluation — Data Catalog</title>
			</Head>
			<SubHeader>
				<SubHeaderLeft>
					<Icon icon='Assessment' size='2x' color='primary' />
					<span className='h4 mb-0 ms-2 fw-bold'>
						Hasil Evaluasi Metadata Quality
					</span>
					<Badge color='primary' isLight className='ms-3'>
						§4.1.6
					</Badge>
				</SubHeaderLeft>
				<SubHeaderRight>
					<Button color='primary' isLight icon='Refresh' onClick={fetchReport}>
						Refresh
					</Button>
					<Button
						color='primary'
						icon='Download'
						className='ms-2'
						onClick={exportCsv}
						isDisable={!report}>
						Export CSV
					</Button>
				</SubHeaderRight>
			</SubHeader>
			<Page>
				<div className='row mb-4'>
					<div className='col-12'>
						<Card shadow='sm'>
							<CardBody>
								<p className='mb-2'>
									Tabel berikut diisi dari <strong>pengamatan lingkungan nyata</strong>{' '}
									(entitas <code>lakehouse_dataset</code> di Apache Atlas), bukan nilai
									contoh statis. Metrik dirata-rata per layer Medallion (Bronze, Silver,
									Gold).
								</p>
								{report?.methodology && (
									<p className='text-muted small mb-0'>{report.methodology}</p>
								)}
								{report?.generatedAt && (
									<p className='text-muted small mb-0 mt-1'>
										Generated: {new Date(report.generatedAt).toLocaleString()}
									</p>
								)}
							</CardBody>
						</Card>
					</div>
				</div>

				<Card shadow='sm'>
					<CardHeader>
						<CardLabel icon='TableChart' iconColor='primary'>
							<CardTitle>Kualitas metadata per layer</CardTitle>
							<CardSubTitle>
								Completeness · Accuracy · Timeliness · Consistency (0–100% dan skor
								1–5)
							</CardSubTitle>
						</CardLabel>
					</CardHeader>
					<CardBody>
						{loading ? (
							<div className='text-center py-5'>
								<Spinner color='primary' size='3rem' />
							</div>
						) : error ? (
							<p className='text-danger mb-0'>{error}</p>
						) : !report || report.layers.every((l) => l.entityCount === 0) ? (
							<p className='text-muted mb-0'>
								Belum ada entitas terdaftar di Atlas untuk layer Bronze/Silver/Gold.
								Jalankan pipeline metadata terlebih dahulu, lalu refresh halaman ini.
							</p>
						) : (
							<div className='table-responsive'>
								<table className='table table-modern table-bordered align-middle'>
									<thead className='table-light'>
										<tr>
											<th>Layer</th>
											<th className='text-center'>Entitas</th>
											<th>Completeness</th>
											<th>Accuracy</th>
											<th>Timeliness</th>
											<th>Consistency</th>
										</tr>
									</thead>
									<tbody>
										{report.layers.map((row: LayerQualityMetrics) => (
											<tr key={row.layer}>
												<td>
													<strong>{row.label}</strong>
													<div className='small text-muted'>
														{row.layer}
													</div>
												</td>
												<td className='text-center'>
													<Badge color='primary' isLight>
														{row.entityCount}
													</Badge>
												</td>
												<td>
													<MetricCell
														pct={row.completeness}
														score15={row.completenessScore15}
													/>
												</td>
												<td>
													<MetricCell
														pct={row.accuracy}
														score15={row.accuracyScore15}
													/>
												</td>
												<td>
													<MetricCell
														pct={row.timeliness}
														score15={row.timelinessScore15}
													/>
												</td>
												<td>
													<MetricCell
														pct={row.consistency}
														score15={row.consistencyScore15}
													/>
												</td>
											</tr>
										))}
									</tbody>
								</table>
							</div>
						)}
					</CardBody>
				</Card>

				<div className='row mt-4'>
					<div className='col-md-6'>
						<Card shadow='sm'>
							<CardHeader>
								<CardLabel icon='Info' iconColor='info'>
									<CardTitle>Definisi dimensi</CardTitle>
								</CardLabel>
							</CardHeader>
							<CardBody className='small'>
								<ul className='mb-0'>
									<li>
										<strong>Completeness</strong> — kelengkapan atribut metadata
										wajib per layer (skema, lokasi, profiling, business, KPI).
									</li>
									<li>
										<strong>Accuracy</strong> — validitas skema, skor kualitas
										Silver, atau kelengkapan metadata KPI/star schema di Gold.
									</li>
									<li>
										<strong>Timeliness</strong> — kesegaran{' '}
										<code>enriched_at</code> / <code>ingested_at</code>.
									</li>
									<li>
										<strong>Consistency</strong> — keselarasan layer,{' '}
										<code>qualifiedName</code>, path storage, dan klasifikasi
										Atlas.
									</li>
								</ul>
							</CardBody>
						</Card>
					</div>
					<div className='col-md-6'>
						<Card shadow='sm'>
							<CardHeader>
								<CardLabel icon='School' iconColor='primary'>
									<CardTitle>Untuk penulisan §4.1.6</CardTitle>
								</CardLabel>
							</CardHeader>
							<CardBody className='small'>
								<p>
									Salin angka persen dari tabel ke skripsi. Contoh kalimat:{' '}
									<em>
										“Layer Silver mencapai completeness {report?.layers[1]?.completeness ?? '—'}%
										dan consistency {report?.layers[1]?.consistency ?? '—'}% pada
										{report?.layers[1]?.entityCount ?? 0} entitas yang diamati.”
									</em>
								</p>
								<p className='mb-0'>
									Gunakan <strong>Export CSV</strong> untuk lampiran atau grafik
									di Word/LaTeX.
								</p>
							</CardBody>
						</Card>
					</div>
				</div>
			</Page>
		</PageWrapper>
	);
};

export const getStaticProps: GetStaticProps = async ({ locale }) => ({
	props: {
		...(await serverSideTranslations(locale || 'en', ['common', 'menu'])),
	},
});

export default MetadataQualityPage;
