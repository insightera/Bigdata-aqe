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
import Progress from '../../components/bootstrap/Progress';
import Spinner from '../../components/bootstrap/Spinner';
import { classificationColor } from '../../helpers/atlasApi';

const ALL_CLASSIFICATIONS = [
	{ name: 'PII', group: 'Security', desc: 'Personally Identifiable Information', icon: 'Security' },
	{ name: 'Staging_Layer', group: 'Layer', desc: 'Data di Staging layer (landing)', icon: 'CloudUpload' },
	{ name: 'Bronze_Layer', group: 'Layer', desc: 'Data di Bronze layer (raw Iceberg)', icon: 'Storage' },
	{ name: 'Silver_Layer', group: 'Layer', desc: 'Data di Silver layer (enriched)', icon: 'AutoFixHigh' },
	{ name: 'Gold_Layer', group: 'Layer', desc: 'Data di Gold layer (star schema)', icon: 'Star' },
	{ name: 'Quality_Pass', group: 'Quality', desc: 'Data quality ≥ 80%', icon: 'CheckCircle' },
	{ name: 'Quality_Quarantine', group: 'Quality', desc: 'Data quality 60-79%', icon: 'Warning' },
	{ name: 'KPI_Metric', group: 'Business', desc: 'Tabel berisi metrik KPI/IKU', icon: 'BarChart' },
	{ name: 'Star_Schema_Dimension', group: 'Schema', desc: 'Tabel dimensi star schema', icon: 'ViewColumn' },
	{ name: 'Star_Schema_Fact', group: 'Schema', desc: 'Tabel fakta star schema', icon: 'TableChart' },
	{ name: 'Executive_Dashboard', group: 'Consumption', desc: 'Data untuk Dashboard Pimpinan', icon: 'Dashboard' },
];

const ClassificationsPage: NextPage = () => {
	const router = useRouter();
	const [counts, setCounts] = useState<Record<string, number>>({});
	const [loading, setLoading] = useState(true);

	const fetchCounts = useCallback(async () => {
		setLoading(true);
		try {
			const promises = ALL_CLASSIFICATIONS.map(async (c) => {
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
			const results = await Promise.all(promises);
			setCounts(Object.fromEntries(results));
		} catch {
			// ignore
		} finally {
			setLoading(false);
		}
	}, []);

	useEffect(() => {
		fetchCounts();
	}, [fetchCounts]);

	const totalEntities = Object.values(counts).reduce((a, b) => a + b, 0);
	const groups = [...new Set(ALL_CLASSIFICATIONS.map((c) => c.group))];

	return (
		<PageWrapper>
			<Head>
				<title>Classifications — Data Catalog</title>
			</Head>
			<SubHeader>
				<SubHeaderLeft>
					<Icon icon='Label' size='2x' color='primary' />
					<span className='h4 mb-0 ms-2 fw-bold'>Classifications</span>
					<Badge color='primary' isLight className='ms-3'>
						{ALL_CLASSIFICATIONS.length} classification types
					</Badge>
				</SubHeaderLeft>
			</SubHeader>
			<Page>
				{loading ? (
					<div className='text-center py-5'>
						<Spinner color='primary' size='3rem' />
					</div>
				) : (
					<>
						{groups.map((group) => {
							const items = ALL_CLASSIFICATIONS.filter(
								(c) => c.group === group,
							);
							return (
								<div key={group} className='row mb-4'>
									<div className='col-12'>
										<Card shadow='sm'>
											<CardHeader>
												<CardLabel
													icon={
														group === 'Layer'
															? 'Layers'
															: group === 'Quality'
																? 'VerifiedUser'
																: group === 'Security'
																	? 'Security'
																	: group === 'Schema'
																		? 'TableChart'
																		: ('Category' as any)
													}
													iconColor='primary'>
													<CardTitle>{group}</CardTitle>
													<CardSubTitle>
														{items.length} classifications
													</CardSubTitle>
												</CardLabel>
											</CardHeader>
											<CardBody>
												<div className='row'>
													{items.map((c) => (
														<div
															key={c.name}
															className='col-md-4 col-lg-3 mb-3'>
															<Card
																shadow='none'
																borderSize={1}
																className='h-100 cursor-pointer'
																onClick={() =>
																	router.push(
																		`/catalog?classification=${c.name}`,
																	)
																}>
																<CardBody className='text-center'>
																	<Icon
																		icon={c.icon as any}
																		size='2x'
																		color={
																			classificationColor(
																				c.name,
																			) as any
																		}
																	/>
																	<h6 className='mt-2 mb-1'>
																		{c.name.replace(
																			/_/g,
																			' ',
																		)}
																	</h6>
																	<p className='text-muted small mb-2'>
																		{c.desc}
																	</p>
																	<Badge
																		color={
																			classificationColor(
																				c.name,
																			) as any
																		}
																		isLight
																		className='px-3'>
																		{counts[c.name] || 0}{' '}
																		entities
																	</Badge>
																</CardBody>
															</Card>
														</div>
													))}
												</div>
											</CardBody>
										</Card>
									</div>
								</div>
							);
						})}
					</>
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

export default ClassificationsPage;
