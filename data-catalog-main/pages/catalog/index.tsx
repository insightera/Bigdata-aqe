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
	CardActions,
} from '../../components/bootstrap/Card';
import Icon from '../../components/icon/Icon';
import Badge from '../../components/bootstrap/Badge';
import Button from '../../components/bootstrap/Button';
import Input from '../../components/bootstrap/forms/Input';
import InputGroup, { InputGroupText } from '../../components/bootstrap/forms/InputGroup';
import Spinner from '../../components/bootstrap/Spinner';
import { layerFromQualifiedName, layerColor, classificationColor } from '../../helpers/atlasApi';

const LAYER_FILTERS = [
	{ label: 'All', value: '', icon: 'ViewModule' },
	{ label: 'Staging', value: 'Staging_Layer', icon: 'CloudUpload' },
	{ label: 'Bronze', value: 'Bronze_Layer', icon: 'Storage' },
	{ label: 'Silver', value: 'Silver_Layer', icon: 'AutoFixHigh' },
	{ label: 'Gold', value: 'Gold_Layer', icon: 'Star' },
];

const TYPE_FILTERS = [
	{ label: 'Datasets', value: 'lakehouse_dataset' },
	{ label: 'Processes', value: 'lakehouse_etl_process' },
];

interface DatasetItem {
	guid: string;
	typeName: string;
	attributes: Record<string, any>;
	classifications?: { typeName: string }[];
	status: string;
}

const CatalogPage: NextPage = () => {
	const router = useRouter();
	const { classification: queryClassification, q: querySearch } = router.query;

	const [entities, setEntities] = useState<DatasetItem[]>([]);
	const [totalCount, setTotalCount] = useState(0);
	const [loading, setLoading] = useState(true);
	const [searchQuery, setSearchQuery] = useState((querySearch as string) || '');
	const [activeLayer, setActiveLayer] = useState((queryClassification as string) || '');
	const [activeType, setActiveType] = useState('lakehouse_dataset');

	const fetchData = useCallback(async () => {
		setLoading(true);
		try {
			const params = new URLSearchParams({ typeName: activeType, limit: '100' });
			if (activeLayer) params.set('classification', activeLayer);
			if (searchQuery) params.set('query', searchQuery);

			const res = await fetch(`/api/atlas/search?${params}`);
			const data = await res.json();
			setEntities(data.entities || []);
			setTotalCount(data.approximateCount || 0);
		} catch {
			setEntities([]);
			setTotalCount(0);
		} finally {
			setLoading(false);
		}
	}, [activeLayer, activeType, searchQuery]);

	useEffect(() => {
		if (queryClassification && queryClassification !== activeLayer) {
			setActiveLayer(queryClassification as string);
		}
		if (querySearch && querySearch !== searchQuery) {
			setSearchQuery(querySearch as string);
		}
	}, [queryClassification, querySearch]);

	useEffect(() => {
		fetchData();
	}, [fetchData]);

	const handleSearch = (e: React.FormEvent) => {
		e.preventDefault();
		fetchData();
	};

	return (
		<PageWrapper>
			<Head>
				<title>Browse Datasets — Data Catalog</title>
			</Head>
			<SubHeader>
				<SubHeaderLeft>
					<Icon icon='Storage' size='2x' color='primary' />
					<span className='h4 mb-0 ms-2 fw-bold'>Browse Datasets</span>
					<Badge color='primary' isLight className='ms-3'>
						{totalCount} results
					</Badge>
				</SubHeaderLeft>
				<SubHeaderRight>
					<form onSubmit={handleSearch} className='d-flex'>
						<InputGroup>
							<InputGroupText>
								<Icon icon='Search' />
							</InputGroupText>
							<Input
								placeholder='Search datasets...'
								value={searchQuery}
								onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
									setSearchQuery(e.target.value)
								}
								ariaLabel='Search'
							/>
						</InputGroup>
						<Button color='primary' type='submit' className='ms-2'>
							Search
						</Button>
					</form>
				</SubHeaderRight>
			</SubHeader>
			<Page>
				{/* Filters */}
				<div className='row mb-4'>
					<div className='col-12'>
						<Card shadow='sm'>
							<CardBody className='py-3'>
								<div className='d-flex align-items-center flex-wrap gap-2'>
									<strong className='me-2'>Layer:</strong>
									{LAYER_FILTERS.map((f) => (
										<Button
											key={f.value}
											color={activeLayer === f.value ? 'primary' : 'light'}
											isLight={activeLayer !== f.value}
											size='sm'
											icon={f.icon as any}
											onClick={() => setActiveLayer(f.value)}>
											{f.label}
										</Button>
									))}

									<span className='mx-3 border-start' style={{ height: 24 }} />

									<strong className='me-2'>Type:</strong>
									{TYPE_FILTERS.map((f) => (
										<Button
											key={f.value}
											color={activeType === f.value ? 'info' : 'light'}
											isLight={activeType !== f.value}
											size='sm'
											onClick={() => setActiveType(f.value)}>
											{f.label}
										</Button>
									))}
								</div>
							</CardBody>
						</Card>
					</div>
				</div>

				{/* Results */}
				{loading ? (
					<div className='text-center py-5'>
						<Spinner color='primary' size='3rem' />
						<p className='mt-3 text-muted'>Loading from Atlas...</p>
					</div>
				) : entities.length === 0 ? (
					<Card shadow='sm'>
						<CardBody className='text-center py-5'>
							<Icon icon='SearchOff' size='4x' color='primary' />
							<h4 className='mt-3'>No datasets found</h4>
							<p className='text-muted'>
								Try adjusting your filters or search query.
								<br />
								Make sure Atlas is running and pipelines have been executed.
							</p>
						</CardBody>
					</Card>
				) : (
					<div className='row'>
						{entities.map((entity) => {
							const qn = entity.attributes?.qualifiedName || '';
							const layer = layerFromQualifiedName(qn);
							const lColor = layerColor(layer);
							const name = entity.attributes?.name || qn;
							const desc = entity.attributes?.description || '';
							const rowCount = entity.attributes?.row_count;
							const colCount = entity.attributes?.column_count;
							const classifications = entity.classifications || [];
							const isProcess = entity.typeName === 'lakehouse_etl_process';

							return (
								<div key={entity.guid} className='col-md-6 col-lg-4 mb-3'>
									<Card
										shadow='sm'
										className='h-100 cursor-pointer border-start border-3'
										borderColor={lColor as any}
										onClick={() => {
											if (isProcess) return;
											router.push(`/catalog/${encodeURIComponent(qn)}`);
										}}>
										<CardHeader className='pb-0'>
											<CardLabel
												icon={isProcess ? 'AccountTree' : 'TableChart'}
												iconColor={lColor as any}>
												<CardTitle tag='h6' className='mb-0'>
													{name}
												</CardTitle>
											</CardLabel>
											<CardActions>
												<Badge color={lColor as any} isLight>
													{layer}
												</Badge>
											</CardActions>
										</CardHeader>
										<CardBody className='pt-2'>
											{desc && (
												<p className='text-muted small mb-2'>
													{desc.length > 120
														? desc.slice(0, 120) + '…'
														: desc}
												</p>
											)}

											{!isProcess && (
												<div className='d-flex gap-3 mb-2'>
													{rowCount != null && (
														<small>
															<Icon
																icon='TableRows'
																className='me-1'
															/>
															{Number(rowCount).toLocaleString()} rows
														</small>
													)}
													{colCount != null && (
														<small>
															<Icon
																icon='ViewColumn'
																className='me-1'
															/>
															{colCount} cols
														</small>
													)}
												</div>
											)}

											<div className='d-flex flex-wrap gap-1'>
												{classifications.map((c) => (
													<Badge
														key={c.typeName}
														color={
															classificationColor(
																c.typeName,
															) as any
														}
														isLight
														className='small'>
														{c.typeName.replace(/_/g, ' ')}
													</Badge>
												))}
											</div>
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

export default CatalogPage;
