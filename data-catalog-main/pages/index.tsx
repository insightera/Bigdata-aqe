import React, { useCallback, useEffect, useState } from 'react';
import type { NextPage } from 'next';
import { GetStaticProps } from 'next';
import Head from 'next/head';
import { serverSideTranslations } from 'next-i18next/serverSideTranslations';
import PageWrapper from '../layout/PageWrapper/PageWrapper';
import SubHeader, { SubHeaderLeft } from '../layout/SubHeader/SubHeader';
import Page from '../layout/Page/Page';
import Card, {
	CardBody,
	CardHeader,
	CardLabel,
	CardTitle,
	CardSubTitle,
	CardActions,
} from '../components/bootstrap/Card';
import Icon from '../components/icon/Icon';
import Badge from '../components/bootstrap/Badge';
import Progress from '../components/bootstrap/Progress';
import Button from '../components/bootstrap/Button';
import { useRouter } from 'next/router';

interface LayerStats {
	name: string;
	label: string;
	icon: string;
	color: string;
	count: number;
	description: string;
	metadata: string[];
}

const LAYER_CONFIG: LayerStats[] = [
	{
		name: 'Staging_Layer',
		label: 'Staging',
		icon: 'CloudUpload',
		color: 'primary',
		count: 0,
		description: 'Raw source data landing zone',
		metadata: ['Source files', 'CSV format'],
	},
	{
		name: 'Bronze_Layer',
		label: 'Bronze',
		icon: 'Storage',
		color: 'primary',
		count: 0,
		description: 'Raw Iceberg tables with profiling',
		metadata: ['Technical', 'Lineage', 'Profiling', 'Classification'],
	},
	{
		name: 'Silver_Layer',
		label: 'Silver',
		icon: 'AutoFixHigh',
		color: 'primary',
		count: 0,
		description: 'Cleaned & enriched with quality',
		metadata: ['Clean', 'Quality', 'Business', 'Compliance'],
	},
	{
		name: 'Gold_Layer',
		label: 'Gold',
		icon: 'Star',
		color: 'primary',
		count: 0,
		description: 'Star schema for OLAP Dashboard',
		metadata: ['Business', 'KPI', 'AI', 'Consumption', 'Lineage'],
	},
];

const CLASSIFICATION_LIST = [
	{ name: 'PII', icon: 'Security', color: 'primary' },
	{ name: 'KPI_Metric', icon: 'BarChart', color: 'primary' },
	{ name: 'Star_Schema_Fact', icon: 'TableChart', color: 'primary' },
	{ name: 'Star_Schema_Dimension', icon: 'ViewColumn', color: 'primary' },
	{ name: 'Executive_Dashboard', icon: 'Dashboard', color: 'primary' },
	{ name: 'Quality_Pass', icon: 'CheckCircle', color: 'primary' },
	{ name: 'Quality_Quarantine', icon: 'Warning', color: 'primary' },
];

const Index: NextPage = () => {
	const router = useRouter();
	const [layers, setLayers] = useState<LayerStats[]>(LAYER_CONFIG);
	const [totalEntities, setTotalEntities] = useState(0);
	const [totalProcesses, setTotalProcesses] = useState(0);
	const [classificationCounts, setClassificationCounts] = useState<
		Record<string, number>
	>({});
	const [loading, setLoading] = useState(true);

	const fetchStats = useCallback(async () => {
		setLoading(true);
		try {
			const layerPromises = LAYER_CONFIG.map(async (l) => {
				try {
					const res = await fetch(
						`/api/atlas/search?typeName=lakehouse_dataset&classification=${l.name}&limit=1`,
					);
					const data = await res.json();
					return { ...l, count: data.approximateCount || 0 };
				} catch {
					return l;
				}
			});

			const updatedLayers = await Promise.all(layerPromises);
			setLayers(updatedLayers);
			setTotalEntities(updatedLayers.reduce((s, l) => s + l.count, 0));

			try {
				const procRes = await fetch(
					'/api/atlas/search?typeName=lakehouse_etl_process&limit=1',
				);
				const procData = await procRes.json();
				setTotalProcesses(procData.approximateCount || 0);
			} catch {
				setTotalProcesses(0);
			}

			const clsPromises = CLASSIFICATION_LIST.map(async (c) => {
				try {
					const res = await fetch(
						`/api/atlas/search?typeName=lakehouse_dataset&classification=${c.name}&limit=1`,
					);
					const data = await res.json();
					return [c.name, data.approximateCount || 0] as [string, number];
				} catch {
					return [c.name, 0] as [string, number];
				}
			});

			const clsCounts = await Promise.all(clsPromises);
			setClassificationCounts(Object.fromEntries(clsCounts));
		} catch {
			// Atlas not available, use fallback
		} finally {
			setLoading(false);
		}
	}, []);

	useEffect(() => {
		fetchStats();
	}, [fetchStats]);

	return (
		<PageWrapper>
			<Head>
				<title>Data Catalog — Dashboard</title>
			</Head>
			<SubHeader>
				<SubHeaderLeft>
					<Icon icon='Dashboard' size='2x' color='primary' />
					<span className='h4 mb-0 ms-2 fw-bold'>Data Catalog Dashboard</span>
					<Badge color='primary' className='ms-3'>
						Metadata Lakehouse
					</Badge>
				</SubHeaderLeft>
			</SubHeader>
			<Page>
				{/* Top KPI Cards */}
				<div className='row mb-4'>
					<div className='col-md-3'>
						<Card shadow='sm' className='border-0'>
							<CardBody className='text-center py-4'>
								<Icon icon='Storage' size='3x' color='primary' />
								<h2 className='mt-3 mb-1 fw-bold'>{totalEntities}</h2>
								<p className='text-muted mb-0'>Total Datasets</p>
							</CardBody>
						</Card>
					</div>
					<div className='col-md-3'>
						<Card shadow='sm' className='border-0'>
							<CardBody className='text-center py-4'>
								<Icon icon='AccountTree' size='3x' color='primary' />
								<h2 className='mt-3 mb-1 fw-bold'>{totalProcesses}</h2>
								<p className='text-muted mb-0'>Lineage Processes</p>
							</CardBody>
						</Card>
					</div>
					<div className='col-md-3'>
						<Card shadow='sm' className='border-0'>
							<CardBody className='text-center py-4'>
								<Icon icon='Layers' size='3x' color='primary' />
								<h2 className='mt-3 mb-1 fw-bold'>4</h2>
								<p className='text-muted mb-0'>Medallion Layers</p>
							</CardBody>
						</Card>
					</div>
					<div className='col-md-3'>
						<Card shadow='sm' className='border-0'>
							<CardBody className='text-center py-4'>
								<Icon icon='Label' size='3x' color='primary' />
								<h2 className='mt-3 mb-1 fw-bold'>11</h2>
								<p className='text-muted mb-0'>Classifications</p>
							</CardBody>
						</Card>
					</div>
				</div>

				{/* Medallion Layers */}
				<div className='row mb-4'>
					<div className='col-12'>
						<Card shadow='sm'>
							<CardHeader>
								<CardLabel icon='Layers' iconColor='primary'>
									<CardTitle>Medallion Architecture — Layers</CardTitle>
									<CardSubTitle>End-to-end pipeline: Source → Staging → Bronze → Silver → Gold</CardSubTitle>
								</CardLabel>
								<CardActions>
									<Button
										color='primary'
										isLight
										icon='Refresh'
										onClick={fetchStats}
										isDisable={loading}>
										Refresh
									</Button>
								</CardActions>
							</CardHeader>
							<CardBody>
								<div className='row'>
									{layers.map((layer, i) => (
										<div key={layer.name} className='col-md-3 mb-3'>
											<Card
												shadow='none'
												borderSize={1}
												borderColor={layer.color as any}
												className='h-100 cursor-pointer'
												onClick={() =>
													router.push(
														`/catalog?classification=${layer.name}`,
													)
												}>
												<CardBody className='text-center'>
													<div className='d-flex justify-content-center align-items-center mb-2'>
														{i > 0 && (
															<Icon
																icon='ArrowForward'
																color='dark'
																className='me-2 opacity-25'
															/>
														)}
														<Icon
															icon={layer.icon as any}
															size='3x'
															color={layer.color as any}
														/>
													</div>
													<h4 className='mb-1'>
														<Badge color={layer.color as any} isLight>
															{layer.label}
														</Badge>
													</h4>
													<h3 className='fw-bold mb-1'>{layer.count}</h3>
													<small className='text-muted'>
														{layer.description}
													</small>
													<div className='mt-3'>
														{layer.metadata.map((m) => (
															<Badge
																key={m}
																color='light'
																className='me-1 mb-1'>
																{m}
															</Badge>
														))}
													</div>
												</CardBody>
											</Card>
										</div>
									))}
								</div>

								{/* Pipeline flow visualization */}
								<div className='mt-3 p-3 bg-l10-primary rounded-3'>
									<div className='d-flex align-items-center justify-content-center flex-wrap'>
										<Badge color='primary' isLight className='px-3 py-2'>
											<Icon icon='CloudUpload' className='me-1' /> Source CSV
										</Badge>
										<Icon icon='ArrowForward' className='mx-2' />
										<Badge color='primary' isLight className='px-3 py-2'>
											<Icon icon='CloudUpload' className='me-1' /> Staging
										</Badge>
										<Icon icon='ArrowForward' className='mx-2' />
										<Badge color='primary' isLight className='px-3 py-2'>
											<Icon icon='Storage' className='me-1' /> Bronze (Iceberg)
										</Badge>
										<Icon icon='ArrowForward' className='mx-2' />
										<Badge color='primary' isLight className='px-3 py-2'>
											<Icon icon='AutoFixHigh' className='me-1' /> Silver
											(Enriched)
										</Badge>
										<Icon icon='ArrowForward' className='mx-2' />
										<Badge color='primary' isLight className='px-3 py-2'>
											<Icon icon='Star' className='me-1' /> Gold (Star
											Schema)
										</Badge>
										<Icon icon='ArrowForward' className='mx-2' />
										<Badge color='primary' className='px-3 py-2'>
											<Icon icon='Dashboard' className='me-1' /> Dashboard
											Pimpinan
										</Badge>
									</div>
								</div>
							</CardBody>
						</Card>
					</div>
				</div>

				<div className='row'>
					{/* Classifications */}
					<div className='col-md-6 mb-4'>
						<Card shadow='sm' stretch>
							<CardHeader>
								<CardLabel icon='Label' iconColor='warning'>
									<CardTitle>Classifications</CardTitle>
									<CardSubTitle>Data governance tags</CardSubTitle>
								</CardLabel>
							</CardHeader>
							<CardBody>
								{CLASSIFICATION_LIST.map((c) => (
									<div
										key={c.name}
										className='d-flex align-items-center justify-content-between py-2 border-bottom cursor-pointer'
										role='button'
										tabIndex={0}
										onClick={() =>
											router.push(
												`/catalog?classification=${c.name}`,
											)
										}
										onKeyDown={(e) => {
											if (e.key === 'Enter')
												router.push(
													`/catalog?classification=${c.name}`,
												);
										}}>
										<div className='d-flex align-items-center'>
											<Icon
												icon={c.icon as any}
												color={c.color as any}
												className='me-2'
											/>
											<span>{c.name.replace(/_/g, ' ')}</span>
										</div>
										<Badge color={c.color as any} isLight>
											{classificationCounts[c.name] || 0}
										</Badge>
									</div>
								))}
							</CardBody>
						</Card>
					</div>

					{/* Metadata Coverage */}
					<div className='col-md-6 mb-4'>
						<Card shadow='sm' stretch>
							<CardHeader>
								<CardLabel icon='Assessment' iconColor='success'>
									<CardTitle>Metadata Coverage</CardTitle>
									<CardSubTitle>Per layer enrichment</CardSubTitle>
								</CardLabel>
							</CardHeader>
							<CardBody>
								{[
									{
										label: 'Bronze — Technical',
										value: 100,
										items: ['Schema', 'Profiling', 'PII Tags', 'Lineage'],
									},
									{
										label: 'Silver — Quality',
										value: 100,
										items: [
											'Quality Score',
											'Business Context',
											'Compliance',
											'Glossary',
										],
									},
									{
										label: 'Gold — KPI',
										value: 100,
										items: [
											'KPI Formula',
											'Consumption',
											'AI Readiness',
											'Star Schema',
										],
									},
								].map((item) => (
									<div key={item.label} className='mb-4'>
										<div className='d-flex justify-content-between mb-1'>
											<strong>{item.label}</strong>
											<span className='text-success fw-bold'>
												{item.value}%
											</span>
										</div>
										<Progress
											value={item.value}
											color='primary'
											height={8}
											isAnimated
										/>
										<div className='mt-1'>
											{item.items.map((tag) => (
												<Badge
													key={tag}
													color='light'
													className='me-1 mt-1'>
													{tag}
												</Badge>
											))}
										</div>
									</div>
								))}

								<div className='mt-4 p-3 bg-l10-primary rounded-3'>
									<h6 className='mb-2'>
										<Icon icon='Info' className='me-1' /> Metadata per Layer
									</h6>
									<small>
										<strong>Bronze:</strong> Technical, Lineage, Profiling,
										Classification
										<br />
										<strong>Silver:</strong> Clean, Quality, Transform
										Lineage, Business, Compliance
										<br />
										<strong>Gold:</strong> Business, KPI, AI, Consumption,
										Advanced Lineage
									</small>
								</div>
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

export default Index;
