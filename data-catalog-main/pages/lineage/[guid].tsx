import React, { useCallback, useEffect, useState } from 'react';
import type { NextPage } from 'next';
import { GetServerSideProps } from 'next';
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
import MedallionLineageFlow from '../../components/lineage/MedallionLineageFlow';
import { layerColor } from '../../helpers/atlasApi';
import {
	parseLineageEntity,
	resolveDisplayName,
	resolveEntityLayer,
	type ParsedLineageNode,
} from '../../helpers/lineageDisplay';

interface LineageEdge {
	from: string;
	to: string;
}

const LineageDetailPage: NextPage = () => {
	const router = useRouter();
	const { guid } = router.query;

	const [baseEntity, setBaseEntity] = useState<any>(null);
	const [nodes, setNodes] = useState<ParsedLineageNode[]>([]);
	const [edges, setEdges] = useState<LineageEdge[]>([]);
	const [loading, setLoading] = useState(true);
	const [error, setError] = useState('');

	const fetchLineage = useCallback(async () => {
		if (!guid) return;
		setLoading(true);
		try {
			const [entityRes, lineageRes] = await Promise.all([
				fetch(`/api/atlas/entity/${guid}`),
				fetch(`/api/atlas/lineage/${guid}?depth=5&direction=BOTH`),
			]);
			const entityData = await entityRes.json();
			const lineageData = await lineageRes.json();

			setBaseEntity(entityData.entity);

			const nodeMap = lineageData.guidEntityMap || {};
			const parsedNodes: ParsedLineageNode[] = Object.entries(nodeMap).map(
				([g, e]: [string, any]) => parseLineageEntity(g, e),
			);

			const parsedEdges: LineageEdge[] = (lineageData.relations || []).map(
				(r: any) => ({
					from: r.fromEntityId,
					to: r.toEntityId,
				}),
			);

			setNodes(parsedNodes);
			setEdges(parsedEdges);
		} catch (err: any) {
			setError(err.message);
		} finally {
			setLoading(false);
		}
	}, [guid]);

	useEffect(() => {
		fetchLineage();
	}, [fetchLineage]);

	if (loading) {
		return (
			<PageWrapper>
				<Page>
					<div className='text-center py-5'>
						<Spinner color='primary' size='3rem' />
						<p className='mt-3 text-muted'>Loading lineage from Atlas...</p>
					</div>
				</Page>
			</PageWrapper>
		);
	}

	const baseAttrs = baseEntity?.attributes || {};
	const baseName = resolveDisplayName(
		baseAttrs.name,
		baseEntity?.typeName || '',
		baseAttrs,
	);
	const baseLayer = resolveEntityLayer(
		baseAttrs.qualifiedName || '',
		baseEntity?.typeName || '',
		baseAttrs,
	);

	const nodeByGuid = new Map(nodes.map((n) => [n.guid, n]));

	return (
		<PageWrapper>
			<Head>
				<title>Lineage: {baseName} — Data Catalog</title>
			</Head>
			<SubHeader>
				<SubHeaderLeft>
					<Button
						color='light'
						icon='ArrowBack'
						onClick={() => router.push('/lineage')}>
						Back
					</Button>
					<Icon
						icon='AccountTree'
						size='2x'
						color={layerColor(baseLayer.startsWith('etl') ? 'dark' : baseLayer) as any}
						className='ms-3'
					/>
					<span className='h4 mb-0 ms-2 fw-bold'>Lineage: {baseName}</span>
					<Badge
						color={
							layerColor(baseLayer.startsWith('etl') ? 'dark' : baseLayer) as any
						}
						className='ms-2'>
						{nodes.find((n) => n.guid === guid)?.layerLabel || baseLayer}
					</Badge>
				</SubHeaderLeft>
				<SubHeaderRight>
					<Badge color='primary' isLight className='px-3 py-2'>
						{nodes.length} entitas · {edges.length} relasi
					</Badge>
				</SubHeaderRight>
			</SubHeader>
			<Page>
				{error && (
					<Card shadow='sm' className='mb-4'>
						<CardBody className='text-center py-4'>
							<Icon icon='Warning' size='3x' color='primary' />
							<h5 className='mt-2'>Lineage data unavailable</h5>
							<p className='text-muted'>{error}</p>
						</CardBody>
					</Card>
				)}

				<Card shadow='sm' className='mb-4'>
					<CardHeader>
						<CardLabel icon='AccountTree' iconColor='primary'>
							<CardTitle>Lineage — alur Medallion</CardTitle>
							<CardSubTitle>
								ETL diposisikan di antara layer (staging_to_bronze, bronze_to_silver,
								silver_to_gold), bukan digabung satu kolom
							</CardSubTitle>
						</CardLabel>
					</CardHeader>
					<CardBody>
						<MedallionLineageFlow
							nodes={nodes}
							highlightGuid={guid as string}
							showMetadataTypes
							maxDatasetsPerLayer={8}
						/>
					</CardBody>
				</Card>

				{edges.length > 0 && (
					<Card shadow='sm'>
						<CardHeader>
							<CardLabel icon='Link' iconColor='info'>
								<CardTitle>Relasi lineage (Atlas)</CardTitle>
							</CardLabel>
						</CardHeader>
						<CardBody>
							<div className='table-responsive'>
								<table className='table table-modern'>
									<thead>
										<tr>
											<th>Dari</th>
											<th />
											<th>Ke</th>
										</tr>
									</thead>
									<tbody>
										{edges.map((edge, i) => {
											const fromNode = nodeByGuid.get(edge.from);
											const toNode = nodeByGuid.get(edge.to);
											const fromLayer = fromNode?.layer || 'metadata';
											const toLayer = toNode?.layer || 'metadata';
											return (
												<tr key={i}>
													<td>
														<Badge
															color={
																layerColor(
																	fromLayer.startsWith('etl')
																		? 'dark'
																		: fromLayer,
																) as any
															}
															isLight
															className='me-2'>
															{fromNode?.layerLabel || 'Metadata'}
														</Badge>
														{fromNode?.displayName || edge.from}
													</td>
													<td className='text-center'>
														<Icon icon='ArrowForward' color='primary' />
													</td>
													<td>
														<Badge
															color={
																layerColor(
																	toLayer.startsWith('etl')
																		? 'dark'
																		: toLayer,
																) as any
															}
															isLight
															className='me-2'>
															{toNode?.layerLabel || 'Metadata'}
														</Badge>
														{toNode?.displayName || edge.to}
													</td>
												</tr>
											);
										})}
									</tbody>
								</table>
							</div>
						</CardBody>
					</Card>
				)}
			</Page>
		</PageWrapper>
	);
};

export const getServerSideProps: GetServerSideProps = async ({ locale }) => ({
	props: {
		// @ts-ignore
		...(await serverSideTranslations(locale, ['common', 'menu'])),
	},
});

export default LineageDetailPage;
