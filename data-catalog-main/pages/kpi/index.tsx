import React, { useCallback, useEffect, useState } from 'react';
import type { NextPage } from 'next';
import { GetStaticProps } from 'next';
import Head from 'next/head';
import { useRouter } from 'next/router';
import { serverSideTranslations } from 'next-i18next/serverSideTranslations';
import PageWrapper from '../../layout/PageWrapper/PageWrapper';
import SubHeader, { SubHeaderLeft } from '../../layout/SubHeader/SubHeader';
import Page from '../../layout/Page/Page';
import Card, {
	CardBody,
	CardHeader,
	CardLabel,
	CardTitle,
	CardSubTitle,
	CardActions,
} from '../../components/bootstrap/Card';
import Icon from '../../components/icon/Icon';
import Badge from '../../components/bootstrap/Badge';
import Button from '../../components/bootstrap/Button';
import Progress from '../../components/bootstrap/Progress';
import Spinner from '../../components/bootstrap/Spinner';
import type { AtlasEntity } from '../../helpers/atlasApi';
import {
	LEADERSHIP_CONSUMERS,
	kpiStatusColor,
	matchIkuEntity,
	mergeAtlasEntities,
	parseProfiling,
} from '../../helpers/kpiAtlas';

const IKU_DEFINITIONS = [
	{
		code: 'IKU-1',
		name: 'Lulusan bekerja/studi/wirausaha',
		icon: 'School',
		color: 'primary',
		target2024: 78,
		fact: 'fact_iku1_lulusan',
	},
	{
		code: 'IKU-2',
		name: 'Mahasiswa MBKM / prestasi nasional',
		icon: 'EmojiEvents',
		color: 'primary',
		target2024: 35,
		fact: 'fact_iku2_mbkm',
	},
	{
		code: 'IKU-3',
		name: 'Dosen tridarma luar/praktisi',
		icon: 'Groups',
		color: 'primary',
		target2024: 25,
		fact: 'fact_iku3_dosen_tridarma',
	},
	{
		code: 'IKU-4',
		name: 'Dosen S3/sertifikat/praktisi',
		icon: 'WorkspacePremium',
		color: 'primary',
		target2024: 50,
		fact: 'fact_iku4_kualifikasi_dosen',
	},
	{
		code: 'IKU-5',
		name: 'Rasio output penelitian intl per dosen',
		icon: 'Science',
		color: 'primary',
		target2024: 0.25,
		fact: 'fact_iku5_penelitian_pkm',
	},
	{
		code: 'IKU-6',
		name: 'Prodi bekerjasama mitra',
		icon: 'Handshake',
		color: 'primary',
		target2024: 60,
		fact: 'fact_iku6_kerjasama_prodi',
	},
	{
		code: 'IKU-7',
		name: 'MK case method / team-based',
		icon: 'AutoStories',
		color: 'primary',
		target2024: 40,
		fact: 'fact_iku7_metode_pembelajaran',
	},
	{
		code: 'IKU-8',
		name: 'Prodi akreditasi internasional',
		icon: 'Public',
		color: 'primary',
		target2024: 3.0,
		fact: 'fact_iku8_akreditasi_internasional',
	},
];

async function fetchAtlasSearch(params: Record<string, string>) {
	const qs = new URLSearchParams(params).toString();
	const res = await fetch(`/api/atlas/search?${qs}`);
	if (!res.ok) return { entities: [] as AtlasEntity[], approximateCount: 0 };
	return res.json();
}

function formatCapaian(value: number | null | undefined, code: string): string {
	if (value == null || Number.isNaN(value)) return '—';
	if (code === 'IKU-5') return value.toFixed(2);
	return `${value.toFixed(1)}%`;
}

const KpiDashboard: NextPage = () => {
	const router = useRouter();
	const [goldEntities, setGoldEntities] = useState<AtlasEntity[]>([]);
	const [loading, setLoading] = useState(true);
	const [goldCount, setGoldCount] = useState(0);
	const [fetchError, setFetchError] = useState<string | null>(null);

	const fetchKpiData = useCallback(async () => {
		setLoading(true);
		setFetchError(null);
		try {
			const [kpiRes, goldRes, queryRes] = await Promise.all([
				fetchAtlasSearch({
					typeName: 'lakehouse_dataset',
					classification: 'KPI_Metric',
					limit: '50',
				}),
				fetchAtlasSearch({
					typeName: 'lakehouse_dataset',
					classification: 'Gold_Layer',
					limit: '100',
				}),
				fetchAtlasSearch({
					typeName: 'lakehouse_dataset',
					query: 'gold.fact_iku',
					limit: '50',
				}),
			]);

			const merged = mergeAtlasEntities(
				kpiRes.entities || [],
				goldRes.entities || [],
				queryRes.entities || [],
			).filter(
				(e) =>
					e.attributes?.layer === 'gold' ||
					(e.attributes?.qualifiedName || '').startsWith('gold.'),
			);

			setGoldEntities(merged);
			setGoldCount(goldRes.approximateCount || merged.length);
		} catch (err: any) {
			setGoldEntities([]);
			setFetchError(err?.message || 'Gagal memuat metadata Atlas');
		} finally {
			setLoading(false);
		}
	}, []);

	useEffect(() => {
		fetchKpiData();
	}, [fetchKpiData]);

	const matchedIkuCount = IKU_DEFINITIONS.filter((iku) =>
		matchIkuEntity(goldEntities, iku),
	).length;

	return (
		<PageWrapper>
			<Head>
				<title>KPI Dashboard — Data Catalog</title>
			</Head>
			<SubHeader>
				<SubHeaderLeft>
					<Icon icon='BarChart' size='2x' color='primary' />
					<span className='h4 mb-0 ms-2 fw-bold'>KPI Dashboard — IKU ITERA</span>
					<Badge color='primary' isLight className='ms-3'>
						Gold Layer Star Schema
					</Badge>
				</SubHeaderLeft>
			</SubHeader>
			<Page>
				{fetchError && (
					<div className='alert alert-warning mb-4' role='alert'>
						{fetchError}. Pastikan Atlas berjalan dan pipeline Gold sudah
						dijalankan.
					</div>
				)}

				<div className='row mb-4'>
					<div className='col-md-3'>
						<Card shadow='sm'>
							<CardBody className='text-center py-4'>
								<Icon icon='Star' size='3x' color='primary' />
								<h2 className='mt-3 mb-1 fw-bold'>{goldCount}</h2>
								<p className='text-muted mb-0'>Gold Tables</p>
							</CardBody>
						</Card>
					</div>
					<div className='col-md-3'>
						<Card shadow='sm'>
							<CardBody className='text-center py-4'>
								<Icon icon='BarChart' size='3x' color='primary' />
								<h2 className='mt-3 mb-1 fw-bold'>{matchedIkuCount}/8</h2>
								<p className='text-muted mb-0'>IKU Terdaftar Atlas</p>
							</CardBody>
						</Card>
					</div>
					<div className='col-md-3'>
						<Card shadow='sm'>
							<CardBody className='text-center py-4'>
								<Icon icon='ViewColumn' size='3x' color='primary' />
								<h2 className='mt-3 mb-1 fw-bold'>5</h2>
								<p className='text-muted mb-0'>Dimensions</p>
							</CardBody>
						</Card>
					</div>
					<div className='col-md-3'>
						<Card shadow='sm'>
							<CardBody className='text-center py-4'>
								<Icon icon='TableChart' size='3x' color='primary' />
								<h2 className='mt-3 mb-1 fw-bold'>10</h2>
								<p className='text-muted mb-0'>Fact Tables</p>
							</CardBody>
						</Card>
					</div>
				</div>

				<div className='row mb-4'>
					<div className='col-12'>
						<Card shadow='sm'>
							<CardHeader>
								<CardLabel icon='Assessment' iconColor='primary'>
									<CardTitle>Indikator Kinerja Utama (IKU)</CardTitle>
									<CardSubTitle>
										Capaian dari fact tables Gold — metadata KPI di Atlas
									</CardSubTitle>
								</CardLabel>
								<CardActions>
									<Button
										color='primary'
										isLight
										icon='Refresh'
										onClick={fetchKpiData}
										isDisable={loading}>
										Refresh
									</Button>
								</CardActions>
							</CardHeader>
							<CardBody>
								{loading ? (
									<div className='text-center py-5'>
										<Spinner color='primary' size='3rem' />
									</div>
								) : matchedIkuCount === 0 ? (
									<div className='text-center py-4 text-muted'>
										<Icon icon='Info' size='3x' className='mb-3' />
										<p className='mb-2'>
											Belum ada entitas IKU di Atlas. Jalankan pipeline{' '}
											<code>silver_to_gold</code> lalu registrasi metadata
											Gold.
										</p>
									</div>
								) : (
									<div className='row'>
										{IKU_DEFINITIONS.map((iku) => {
											const matched = matchIkuEntity(goldEntities, iku);
											const profiling = parseProfiling(matched);
											const kpiMeta = profiling.kpi || {};
											const rowCount = matched?.attributes?.row_count || 0;

											const capaian = kpiMeta.nilai_capaian ?? null;
											const target =
												kpiMeta.nilai_target ?? iku.target2024;
											const status = kpiMeta.status_capaian as
												| string
												| undefined;
											const satuan =
												kpiMeta.satuan ||
												(iku.code === 'IKU-5' ? 'Rasio' : '%');

											const progressPct =
												capaian != null && target
													? Math.min(
															iku.code === 'IKU-5'
																? (capaian / target) * 100
																: capaian,
															100,
														)
													: 0;

											return (
												<div
													key={iku.code}
													className='col-md-6 col-lg-3 mb-3'>
													<Card
														shadow='none'
														borderSize={1}
														borderColor={iku.color as any}
														className='h-100'>
														<CardBody>
															<div className='d-flex align-items-center mb-2'>
																<Icon
																	icon={iku.icon as any}
																	size='2x'
																	color={
																		iku.color as any
																	}
																/>
																<div className='ms-2'>
																	<Badge
																		color={
																			iku.color as any
																		}>
																		{iku.code}
																	</Badge>
																	{status && (
																		<Badge
																			color={kpiStatusColor(
																				status,
																			)}
																			isLight
																			className='ms-1'>
																			{status}
																		</Badge>
																	)}
																</div>
															</div>

															<h6 className='mb-3'>
																{iku.name}
															</h6>

															<div className='mb-2'>
																<div className='d-flex justify-content-between'>
																	<small className='text-muted'>
																		Capaian
																	</small>
																	<strong>
																		{formatCapaian(
																			capaian,
																			iku.code,
																		)}
																	</strong>
																</div>
																<div className='d-flex justify-content-between mb-1'>
																	<small className='text-muted'>
																		Target
																	</small>
																	<small>
																		{formatCapaian(
																			target,
																			iku.code,
																		)}{' '}
																		{satuan}
																	</small>
																</div>
																<Progress
																	value={progressPct}
																	color={
																		iku.color as any
																	}
																	height={6}
																/>
															</div>

															{kpiMeta.formula && (
																<div className='mb-2'>
																	<small className='text-muted d-block'>
																		Formula
																	</small>
																	<code className='small'>
																		{kpiMeta.formula}
																	</code>
																</div>
															)}

															<div className='d-flex justify-content-between align-items-center mt-3'>
																<small className='text-muted'>
																	{rowCount
																		? `${Number(
																				rowCount,
																			).toLocaleString()} baris`
																		: matched
																			? '0 baris'
																			: 'Belum terdaftar'}
																</small>
																{matched && (
																	<Button
																		color={
																			iku.color as any
																		}
																		isLight
																		size='sm'
																		onClick={() =>
																			router.push(
																				`/catalog/${encodeURIComponent(
																					matched
																						.attributes
																						?.qualifiedName,
																				)}`,
																			)
																		}>
																		Detail
																	</Button>
																)}
															</div>
														</CardBody>
													</Card>
												</div>
											);
										})}
									</div>
								)}
							</CardBody>
						</Card>
					</div>
				</div>

				<div className='row'>
					<div className='col-md-6 mb-4'>
						<Card shadow='sm' stretch>
							<CardHeader>
								<CardLabel icon='Star' iconColor='success'>
									<CardTitle>Star Schema Design</CardTitle>
								</CardLabel>
							</CardHeader>
							<CardBody>
								<div className='p-3 bg-l10-primary rounded-3 text-center'>
									<h6 className='mb-3'>Gold Layer Architecture</h6>
									<div className='d-flex justify-content-center gap-2 flex-wrap mb-3'>
										{[
											'dim_waktu',
											'dim_prodi',
											'dim_dosen',
											'dim_mahasiswa',
											'dim_topik',
										].map((dim) => (
											<Badge
												key={dim}
												color='primary'
												isLight
												className='px-3 py-2'>
												<Icon icon='ViewColumn' className='me-1' />
												{dim}
											</Badge>
										))}
									</div>
									<Icon
										icon='ArrowDownward'
										size='2x'
										className='my-2'
										color='primary'
									/>
									<div className='d-flex justify-content-center gap-2 flex-wrap'>
										{IKU_DEFINITIONS.map((iku) => (
											<Badge
												key={iku.code}
												color={iku.color as any}
												isLight
												className='px-2 py-1'>
												{iku.code}
											</Badge>
										))}
										<Badge color='dark' isLight className='px-2 py-1'>
											SAKIP
										</Badge>
										<Badge color='dark' isLight className='px-2 py-1'>
											Rekap
										</Badge>
									</div>
								</div>
							</CardBody>
						</Card>
					</div>

					<div className='col-md-6 mb-4'>
						<Card shadow='sm' stretch>
							<CardHeader>
								<CardLabel icon='People' iconColor='primary'>
									<CardTitle>Dashboard Consumers</CardTitle>
								</CardLabel>
							</CardHeader>
							<CardBody>
								{LEADERSHIP_CONSUMERS.map((consumer) => (
									<div
										key={consumer.role}
										className='d-flex align-items-center py-2 border-bottom'>
										<Icon
											icon={consumer.icon as any}
											color='primary'
											className='me-3'
										/>
										<div>
											<strong>{consumer.role}</strong>
											<br />
											<small className='text-muted'>
												{consumer.desc}
											</small>
										</div>
									</div>
								))}
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
		// @ts-ignore
		...(await serverSideTranslations(locale, ['common', 'menu'])),
	},
});

export default KpiDashboard;
