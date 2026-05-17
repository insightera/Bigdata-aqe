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
} from '../../components/bootstrap/Card';
import Icon from '../../components/icon/Icon';
import Badge from '../../components/bootstrap/Badge';
import Button from '../../components/bootstrap/Button';
import Spinner from '../../components/bootstrap/Spinner';
import MedallionLineageFlow from '../../components/lineage/MedallionLineageFlow';
import { layerColor } from '../../helpers/atlasApi';
import {
	MEDALLION_DATA_LAYERS,
	MEDALLION_PIPELINE_EDGES,
	parseLineageEntity,
	type ParsedLineageNode,
} from '../../helpers/lineageDisplay';

const LineageIndexPage: NextPage = () => {
	const router = useRouter();
	const [nodes, setNodes] = useState<ParsedLineageNode[]>([]);
	const [loading, setLoading] = useState(true);

	const fetchDatasets = useCallback(async () => {
		setLoading(true);
		try {
			const [dsRes, procRes] = await Promise.all([
				fetch('/api/atlas/search?typeName=lakehouse_dataset&limit=100'),
				fetch('/api/atlas/search?typeName=lakehouse_etl_process&limit=100'),
			]);
			const dsData = await dsRes.json();
			const procData = await procRes.json();
			const parsed = [
				...(dsData.entities || []).map((e: any) => parseLineageEntity(e.guid, e)),
				...(procData.entities || []).map((e: any) => parseLineageEntity(e.guid, e)),
			];
			setNodes(parsed);
		} catch {
			setNodes([]);
		} finally {
			setLoading(false);
		}
	}, []);

	useEffect(() => {
		fetchDatasets();
	}, [fetchDatasets]);

	const grouped: Record<string, ParsedLineageNode[]> = {};
	MEDALLION_DATA_LAYERS.forEach((l) => {
		grouped[l] = nodes.filter((n) => !n.isProcess && n.layer === l);
	});

	return (
		<PageWrapper>
			<Head>
				<title>Data Lineage — Data Catalog</title>
			</Head>
			<SubHeader>
				<SubHeaderLeft>
					<Icon icon='AccountTree' size='2x' color='primary' />
					<span className='h4 mb-0 ms-2 fw-bold'>Data Lineage</span>
					<Badge color='primary' isLight className='ms-3'>
						Sesuai diagram pipeline + metadata
					</Badge>
				</SubHeaderLeft>
			</SubHeader>
			<Page>
				<div className='row mb-4'>
					<div className='col-12'>
						<Card shadow='sm'>
							<CardHeader>
								<CardLabel icon='AccountTree' iconColor='primary'>
									<CardTitle>Alur Medallion &amp; ETL</CardTitle>
									<CardSubTitle>
										Staging → Bronze → Silver → Gold — setiap panah = satu pipeline
										({MEDALLION_PIPELINE_EDGES.map((e) => e.pipelineName).join(', ')})
									</CardSubTitle>
								</CardLabel>
							</CardHeader>
							<CardBody>
								{loading ? (
									<div className='text-center py-4'>
										<Spinner color='primary' />
									</div>
								) : (
									<MedallionLineageFlow
										nodes={nodes}
										showMetadataTypes
										maxDatasetsPerLayer={3}
									/>
								)}
							</CardBody>
						</Card>
					</div>
				</div>

				{loading ? (
					<div className='text-center py-5'>
						<Spinner color='primary' size='3rem' />
					</div>
				) : (
					<div className='row'>
						{MEDALLION_DATA_LAYERS.map((layer) => {
							const items = grouped[layer] || [];
							if (items.length === 0) return null;
							return (
								<div key={layer} className='col-md-3 mb-4'>
									<Card shadow='sm' stretch>
										<CardHeader>
											<CardLabel
												icon={
													layer === 'staging'
														? 'CloudUpload'
														: layer === 'bronze'
															? 'Storage'
															: layer === 'silver'
																? 'AutoFixHigh'
																: ('Star' as any)
												}
												iconColor={layerColor(layer) as any}>
												<CardTitle>
													{layer.charAt(0).toUpperCase() + layer.slice(1)}
												</CardTitle>
											</CardLabel>
										</CardHeader>
										<CardBody>
											{items.map((item) => (
												<Button
													key={item.guid}
													color={layerColor(layer) as any}
													isLight
													size='sm'
													className='w-100 mb-2 text-start'
													icon='AccountTree'
													onClick={() =>
														router.push(`/lineage/${item.guid}`)
													}>
													{item.displayName}
												</Button>
											))}
										</CardBody>
									</Card>
								</div>
							);
						})}
					</div>
				)}
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

export default LineageIndexPage;
