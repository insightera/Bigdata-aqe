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

const QualityPage: NextPage = () => {
	const router = useRouter();
	const [passCount, setPassCount] = useState(0);
	const [quarantineCount, setQuarantineCount] = useState(0);
	const [silverEntities, setSilverEntities] = useState<any[]>([]);
	const [loading, setLoading] = useState(true);

	const fetchQuality = useCallback(async () => {
		setLoading(true);
		try {
			const [passRes, quarRes, silverRes] = await Promise.all([
				fetch('/api/atlas/search?typeName=lakehouse_dataset&classification=Quality_Pass&limit=1'),
				fetch('/api/atlas/search?typeName=lakehouse_dataset&classification=Quality_Quarantine&limit=1'),
				fetch('/api/atlas/search?typeName=lakehouse_dataset&classification=Silver_Layer&limit=50'),
			]);
			const passData = await passRes.json();
			const quarData = await quarRes.json();
			const silverData = await silverRes.json();
			setPassCount(passData.approximateCount || 0);
			setQuarantineCount(quarData.approximateCount || 0);
			setSilverEntities(silverData.entities || []);
		} catch {
			// ignore
		} finally {
			setLoading(false);
		}
	}, []);

	useEffect(() => {
		fetchQuality();
	}, [fetchQuality]);

	const totalQuality = passCount + quarantineCount;
	const passPercent = totalQuality > 0 ? Math.round((passCount / totalQuality) * 100) : 0;

	return (
		<PageWrapper>
			<Head>
				<title>Data Quality — Data Catalog</title>
			</Head>
			<SubHeader>
				<SubHeaderLeft>
					<Icon icon='VerifiedUser' size='2x' color='primary' />
					<span className='h4 mb-0 ms-2 fw-bold'>Data Quality</span>
					<Badge color='primary' isLight className='ms-3'>
						Silver Layer Quality Metrics
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
						{/* Summary */}
						<div className='row mb-4'>
							<div className='col-md-4'>
								<Card shadow='sm'>
									<CardBody className='text-center py-4'>
										<Icon icon='CheckCircle' size='3x' color='primary' />
										<h2 className='mt-3 mb-1 fw-bold text-success'>
											{passCount}
										</h2>
										<p className='text-muted mb-0'>Quality PASS</p>
										<small className='text-muted'>
											completeness ≥ 80%
										</small>
									</CardBody>
								</Card>
							</div>
							<div className='col-md-4'>
								<Card shadow='sm'>
									<CardBody className='text-center py-4'>
										<Icon icon='Warning' size='3x' color='primary' />
										<h2 className='mt-3 mb-1 fw-bold text-warning'>
											{quarantineCount}
										</h2>
										<p className='text-muted mb-0'>Quality QUARANTINE</p>
										<small className='text-muted'>
											completeness 60-79%
										</small>
									</CardBody>
								</Card>
							</div>
							<div className='col-md-4'>
								<Card shadow='sm'>
									<CardBody className='text-center py-4'>
										<Icon icon='Assessment' size='3x' color='primary' />
										<h2 className='mt-3 mb-1 fw-bold'>
											{passPercent}%
										</h2>
										<p className='text-muted mb-0'>Overall Pass Rate</p>
										<Progress
											value={passPercent}
											color='primary'
											height={8}
											className='mt-2'
											isAnimated
										/>
									</CardBody>
								</Card>
							</div>
						</div>

						{/* Quality rules */}
						<div className='row mb-4'>
							<div className='col-12'>
								<Card shadow='sm'>
									<CardHeader>
										<CardLabel icon='Rule' iconColor='primary'>
											<CardTitle>Quality Rules</CardTitle>
											<CardSubTitle>
												Applied during Bronze → Silver pipeline
											</CardSubTitle>
										</CardLabel>
									</CardHeader>
									<CardBody>
										<div className='table-responsive'>
											<table className='table table-modern'>
												<thead>
													<tr>
														<th>Status</th>
														<th>Completeness Score</th>
														<th>Action</th>
														<th>Classification</th>
													</tr>
												</thead>
												<tbody>
													<tr>
														<td>
															<Badge color='primary'>PASS</Badge>
														</td>
														<td>≥ 80%</td>
														<td>
															Proceed to Silver layer
														</td>
														<td>
															<Badge
																color='primary'
																isLight>
																Quality_Pass
															</Badge>
														</td>
													</tr>
													<tr>
														<td>
															<Badge color='primary'>
																QUARANTINE
															</Badge>
														</td>
														<td>60% — 79%</td>
														<td>
															Flag for review, proceed with
															warning
														</td>
														<td>
															<Badge
																color='primary'
																isLight>
																Quality_Quarantine
															</Badge>
														</td>
													</tr>
													<tr>
														<td>
															<Badge color='primary'>
																REJECT
															</Badge>
														</td>
														<td>&lt; 60%</td>
														<td>
															Reject — do not proceed
														</td>
														<td>
															<span className='text-muted'>
																Not ingested
															</span>
														</td>
													</tr>
												</tbody>
											</table>
										</div>
									</CardBody>
								</Card>
							</div>
						</div>

						{/* Silver entities quality */}
						{silverEntities.length > 0 && (
							<div className='row'>
								<div className='col-12'>
									<Card shadow='sm'>
										<CardHeader>
											<CardLabel icon='TableChart' iconColor='info'>
												<CardTitle>Silver Layer Datasets</CardTitle>
											</CardLabel>
										</CardHeader>
										<CardBody>
											<div className='row'>
												{silverEntities.map((entity) => {
													const name = entity.attributes?.name;
													const classifications =
														entity.classifications || [];
													const isPass = classifications.some(
														(c: any) =>
															c.typeName ===
															'Quality_Pass',
													);
													const isQuar = classifications.some(
														(c: any) =>
															c.typeName ===
															'Quality_Quarantine',
													);

													return (
														<div
															key={entity.guid}
															className='col-md-4 mb-3'>
															<Card
																shadow='none'
																borderSize={1}
																borderColor={
																	isPass
																		? 'success'
																		: isQuar
																			? 'warning'
																			: ('secondary' as any)
																}
																className='cursor-pointer'
																onClick={() =>
																	router.push(
																		`/catalog/${encodeURIComponent(
																			entity.attributes
																				?.qualifiedName,
																		)}`,
																	)
																}>
																<CardBody>
																	<div className='d-flex align-items-center justify-content-between'>
																		<h6 className='mb-0'>
																			{name}
																		</h6>
																		{isPass && (
																			<Badge color='primary'>
																				PASS
																			</Badge>
																		)}
																		{isQuar && (
																			<Badge color='primary'>
																				QUARANTINE
																			</Badge>
																		)}
																	</div>
																</CardBody>
															</Card>
														</div>
													);
												})}
											</div>
										</CardBody>
									</Card>
								</div>
							</div>
						)}
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

export default QualityPage;
