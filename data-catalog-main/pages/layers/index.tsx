import React from 'react';
import { useRouter } from 'next/router';
import type { NextPage } from 'next';
import { GetStaticProps } from 'next';
import Head from 'next/head';
import { serverSideTranslations } from 'next-i18next/serverSideTranslations';
import PageWrapper from '../../layout/PageWrapper/PageWrapper';
import SubHeader, { SubHeaderLeft } from '../../layout/SubHeader/SubHeader';
import Page from '../../layout/Page/Page';
import Card, { CardBody } from '../../components/bootstrap/Card';
import Icon from '../../components/icon/Icon';
import Badge from '../../components/bootstrap/Badge';
import Button from '../../components/bootstrap/Button';

const LAYERS = [
	{
		id: 'staging',
		label: 'Staging',
		icon: 'CloudUpload',
		color: 'primary',
		classification: 'Staging_Layer',
		description: 'Raw source data landing zone. CSV files uploaded from source systems.',
		metadata: ['Source files', 'CSV format', 'No transformation'],
		pipeline: 'Upload from data sources → MinIO staging bucket',
	},
	{
		id: 'bronze',
		label: 'Bronze',
		icon: 'Storage',
		color: 'primary',
		classification: 'Bronze_Layer',
		description: 'Raw Iceberg tables with schema inference and initial profiling.',
		metadata: ['Raw Technical Metadata', 'Raw Lineage', 'Raw Data Profiling', 'Raw Classification (PII)'],
		pipeline: 'staging_to_bronze.py: CSV → Iceberg via PySpark',
	},
	{
		id: 'silver',
		label: 'Silver',
		icon: 'AutoFixHigh',
		color: 'primary',
		classification: 'Silver_Layer',
		description: 'Cleaned, enriched, and quality-checked data with business context.',
		metadata: ['Clean Metadata', 'Quality Metadata', 'Transformation Lineage', 'Business Metadata', 'Compliance Metadata'],
		pipeline: 'bronze_to_silver.py: Quality checks + enrichment + joins',
	},
	{
		id: 'gold',
		label: 'Gold',
		icon: 'Star',
		color: 'primary',
		classification: 'Gold_Layer',
		description: 'Star schema (5 dimensions + 10 facts) for OLAP Dashboard Pimpinan.',
		metadata: ['Business Metadata', 'KPI Metadata', 'AI Metadata', 'Consumption Metadata', 'Advanced Lineage'],
		pipeline: 'silver_to_gold.py: Star schema aggregation + IKU computation',
	},
];

const LayersPage: NextPage = () => {
	const router = useRouter();

	return (
		<PageWrapper>
			<Head>
				<title>Medallion Layers — Data Catalog</title>
			</Head>
			<SubHeader>
				<SubHeaderLeft>
					<Icon icon='Layers' size='2x' color='primary' />
					<span className='h4 mb-0 ms-2 fw-bold'>Medallion Architecture</span>
				</SubHeaderLeft>
			</SubHeader>
			<Page>
				{LAYERS.map((layer, i) => (
					<div key={layer.id} className='row mb-4'>
						<div className='col-12'>
							<Card shadow='sm' borderSize={1} borderColor={layer.color as any}>
								<CardBody>
									<div className='row align-items-center'>
										<div className='col-md-1 text-center'>
											<Icon
												icon={layer.icon as any}
												size='3x'
												color={layer.color as any}
											/>
											{i < LAYERS.length - 1 && (
												<div className='mt-2'>
													<Icon icon='ArrowDownward' color='primary' />
												</div>
											)}
										</div>
										<div className='col-md-7'>
											<h4>
												<Badge color={layer.color as any} className='me-2'>
													{layer.label}
												</Badge>
											</h4>
											<p className='mb-2'>{layer.description}</p>
											<small className='text-muted'>
												<strong>Pipeline:</strong> {layer.pipeline}
											</small>
										</div>
										<div className='col-md-3'>
											<h6 className='mb-2'>Metadata Types</h6>
											{layer.metadata.map((m) => (
												<Badge key={m} color='light' className='me-1 mb-1'>
													{m}
												</Badge>
											))}
										</div>
										<div className='col-md-1 text-end'>
											<Button
												color={layer.color as any}
												isLight
												icon='ArrowForward'
												onClick={() =>
													router.push(
														`/catalog?classification=${layer.classification}`,
													)
												}>
												Browse
											</Button>
										</div>
									</div>
								</CardBody>
							</Card>
						</div>
					</div>
				))}
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

export default LayersPage;
