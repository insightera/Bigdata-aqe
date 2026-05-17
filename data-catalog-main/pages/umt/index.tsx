import React, { useCallback, useEffect, useState } from 'react';
import type { NextPage } from 'next';
import { GetStaticProps } from 'next';
import Head from 'next/head';
import { useRouter } from 'next/router';
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
import Spinner from '../../components/bootstrap/Spinner';
import type { UmtRow } from '../../helpers/umtBuilder';
import { layerColor } from '../../helpers/atlasApi';

const INGESTION_STEPS = [
	{
		layer: 'Staging',
		source: 'CSV / MinIO',
		mechanism: 'Airflow upload + Spark staging_to_bronze',
		atlas: 'register_bronze_metadata.py (staging entity)',
	},
	{
		layer: 'Bronze',
		source: 'Spark profiling (Iceberg)',
		mechanism: 'DAG staging_to_bronze → Atlas REST',
		atlas: 'Technical: schema, path, PII, Bronze_Layer',
	},
	{
		layer: 'Silver',
		source: 'Spark transform + quality',
		mechanism: 'DAG bronze_to_silver → register_silver_metadata.py',
		atlas: 'Business, compliance, Quality_*, enriched_at',
	},
	{
		layer: 'Gold',
		source: 'Star schema ETL',
		mechanism: 'DAG silver_to_gold → register_gold_metadata.py',
		atlas: 'KPI, consumption, lineage ke Silver',
	},
];

const UmtPage: NextPage = () => {
	const router = useRouter();
	const [rows, setRows] = useState<UmtRow[]>([]);
	const [generatedAt, setGeneratedAt] = useState<string>('');
	const [loading, setLoading] = useState(true);
	const [error, setError] = useState<string | null>(null);
	const [expandedGuid, setExpandedGuid] = useState<string | null>(null);
	const [layerFilter, setLayerFilter] = useState('');

	const fetchUmt = useCallback(async () => {
		setLoading(true);
		setError(null);
		try {
			const res = await fetch('/api/atlas/umt?limit=500');
			const data = await res.json();
			if (!res.ok) throw new Error(data.error || 'Failed to load UMT');
			setRows(data.rows || []);
			setGeneratedAt(data.generatedAt || '');
		} catch (e: unknown) {
			setError(e instanceof Error ? e.message : 'Unknown error');
			setRows([]);
		} finally {
			setLoading(false);
		}
	}, []);

	useEffect(() => {
		fetchUmt();
	}, [fetchUmt]);

	const filtered = layerFilter
		? rows.filter((r) => r.layer === layerFilter)
		: rows;

	const downloadJson = () => {
		const blob = new Blob(
			[JSON.stringify({ generated_at: generatedAt, rows: filtered }, null, 2)],
			{ type: 'application/json' },
		);
		const url = URL.createObjectURL(blob);
		const a = document.createElement('a');
		a.href = url;
		a.download = `umt_${new Date().toISOString().slice(0, 10)}.json`;
		a.click();
		URL.revokeObjectURL(url);
	};

	const jsonPreview = (obj: Record<string, unknown>) => {
		const s = JSON.stringify(obj, null, 0);
		return s.length > 80 ? `${s.slice(0, 77)}…` : s;
	};

	return (
		<PageWrapper>
			<Head>
				<title>Unified Metadata Table — Data Catalog</title>
			</Head>
			<SubHeader>
				<SubHeaderLeft>
					<Icon icon='TableChart' size='2x' color='primary' />
					<span className='h4 mb-0 ms-2 fw-bold'>Unified Metadata Table (UMT)</span>
					<Badge color='primary' isLight className='ms-3'>
						§4.1.4
					</Badge>
				</SubHeaderLeft>
				<SubHeaderRight>
					<Button color='primary' isLight icon='Refresh' onClick={fetchUmt}>
						Refresh
					</Button>
					<Button
						color='primary'
						icon='Download'
						className='ms-2'
						onClick={downloadJson}
						isDisable={filtered.length === 0}>
						Export JSON
					</Button>
				</SubHeaderRight>
			</SubHeader>
			<Page>
				{/* Ingestion & enrichment */}
				<div className='row mb-4'>
					<div className='col-lg-6 mb-4'>
						<Card shadow='sm' stretch>
							<CardHeader>
								<CardLabel icon='CloudUpload' iconColor='primary'>
									<CardTitle>Proses ingestion</CardTitle>
									<CardSubTitle>
										Sumber data & metadata → Atlas (REST batch via DAG)
									</CardSubTitle>
								</CardLabel>
							</CardHeader>
							<CardBody>
								<div className='table-responsive'>
									<table className='table table-modern table-sm mb-0'>
										<thead>
											<tr>
												<th>Layer</th>
												<th>Sumber</th>
												<th>Mekanisme</th>
											</tr>
										</thead>
										<tbody>
											{INGESTION_STEPS.map((s) => (
												<tr key={s.layer}>
													<td>
														<strong>{s.layer}</strong>
													</td>
													<td>
														<small>{s.source}</small>
													</td>
													<td>
														<small>{s.mechanism}</small>
													</td>
												</tr>
											))}
										</tbody>
									</table>
								</div>
							</CardBody>
						</Card>
					</div>
					<div className='col-lg-6 mb-4'>
						<Card shadow='sm' stretch>
							<CardHeader>
								<CardLabel icon='AutoFixHigh' iconColor='primary'>
									<CardTitle>Enrichment di Silver</CardTitle>
									<CardSubTitle>
										Klasifikasi, glossary, business metadata, lineage transformasi
									</CardSubTitle>
								</CardLabel>
							</CardHeader>
							<CardBody>
								<ul className='mb-0'>
									<li>
										<strong>Classifications:</strong> Silver_Layer, Quality_Pass /
										Quarantine, PII
									</li>
									<li>
										<strong>Business:</strong> owner, deskripsi IKU, glossary_terms
										→ <code>business_json</code>
									</li>
									<li>
										<strong>Operational:</strong> skor kualitas, compliance →{' '}
										<code>operational_json</code>
									</li>
									<li>
										<strong>Lineage:</strong> proses{' '}
										<code>bronze_to_silver</code> di Atlas (persiapan rantai ke
										Gold)
									</li>
									<li>
										<strong>Timeliness:</strong> <code>last_enriched_at</code>{' '}
										(enriched_at / ingested_at)
									</li>
								</ul>
							</CardBody>
						</Card>
					</div>
				</div>

				{/* UMT table */}
				<Card shadow='sm'>
					<CardHeader>
						<CardLabel icon='ViewList' iconColor='info'>
							<CardTitle>View logis UMT</CardTitle>
							<CardSubTitle>
								Agregasi entitas Atlas (Bronze / Silver / Gold)
								{generatedAt && (
									<span className='ms-2 text-muted'>
										— generated {new Date(generatedAt).toLocaleString()}
									</span>
								)}
							</CardSubTitle>
						</CardLabel>
						<div className='d-flex flex-wrap gap-2'>
							{['', 'staging', 'bronze', 'silver', 'gold'].map((l) => (
								<Button
									key={l || 'all'}
									size='sm'
									color='primary'
									isLight={layerFilter !== l}
									onClick={() => setLayerFilter(l)}>
									{l || 'All'}
								</Button>
							))}
						</div>
					</CardHeader>
					<CardBody>
						{loading ? (
							<div className='text-center py-5'>
								<Spinner color='primary' size='3rem' />
							</div>
						) : error ? (
							<p className='text-danger mb-0'>{error}</p>
						) : filtered.length === 0 ? (
							<p className='text-muted mb-0'>
								Belum ada entitas di Atlas. Jalankan pipeline metadata (Bronze →
								Silver → Gold) terlebih dahulu.
							</p>
						) : (
							<div className='table-responsive'>
								<table className='table table-modern table-hover align-middle'>
									<thead>
										<tr>
											<th>asset_qualified_name</th>
											<th>layer</th>
											<th>technical_json</th>
											<th>business_json</th>
											<th>operational_json</th>
											<th>last_enriched_at</th>
											<th />
										</tr>
									</thead>
									<tbody>
										{filtered.map((row) => (
											<React.Fragment key={row.guid}>
												<tr>
													<td>
														<code className='small'>
															{row.asset_qualified_name}
														</code>
													</td>
													<td>
														<Badge
															color={
																layerColor(row.layer) as any
															}>
															{row.layer}
														</Badge>
													</td>
													<td>
														<small className='text-muted'>
															{jsonPreview(row.technical_json)}
														</small>
													</td>
													<td>
														<small className='text-muted'>
															{jsonPreview(row.business_json)}
														</small>
													</td>
													<td>
														<small className='text-muted'>
															{jsonPreview(row.operational_json)}
														</small>
													</td>
													<td>
														<small>
															{row.last_enriched_at
																? new Date(
																		row.last_enriched_at,
																	).toLocaleString()
																: '—'}
														</small>
													</td>
													<td>
														<Button
															size='sm'
															color='primary'
															isLight
															icon={
																expandedGuid === row.guid
																	? 'ExpandLess'
																	: 'ExpandMore'
															}
															onClick={() =>
																setExpandedGuid(
																	expandedGuid === row.guid
																		? null
																		: row.guid,
																)
															}
														/>
													</td>
												</tr>
												{expandedGuid === row.guid && (
													<tr>
														<td colSpan={7}>
															<div className='row g-3'>
																{(
																	[
																		[
																			'technical_json',
																			row.technical_json,
																		],
																		[
																			'business_json',
																			row.business_json,
																		],
																		[
																			'operational_json',
																			row.operational_json,
																		],
																	] as const
																).map(([label, data]) => (
																	<div
																		className='col-md-4'
																		key={label}>
																		<h6>{label}</h6>
																		<pre
																			className='bg-l10-primary p-2 rounded small mb-0'
																			style={{
																				maxHeight: 200,
																				overflow: 'auto',
																			}}>
																			{JSON.stringify(
																				data,
																				null,
																				2,
																			)}
																		</pre>
																	</div>
																))}
															</div>
															<div className='mt-2'>
																<Button
																	size='sm'
																	color='primary'
																	isLight
																	onClick={() =>
																		router.push(
																			`/catalog/${encodeURIComponent(row.asset_qualified_name)}`,
																		)
																	}>
																	Buka di Catalog
																</Button>
															</div>
														</td>
													</tr>
												)}
											</React.Fragment>
										))}
									</tbody>
								</table>
							</div>
						)}
					</CardBody>
				</Card>
			</Page>
		</PageWrapper>
	);
};

export const getStaticProps: GetStaticProps = async ({ locale }) => ({
	props: {
		...(await serverSideTranslations(locale || 'en', ['common', 'menu'])),
	},
});

export default UmtPage;
