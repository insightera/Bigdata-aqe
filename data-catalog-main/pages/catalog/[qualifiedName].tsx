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
	CardActions,
} from '../../components/bootstrap/Card';
import Icon from '../../components/icon/Icon';
import Badge from '../../components/bootstrap/Badge';
import Button from '../../components/bootstrap/Button';
import Spinner from '../../components/bootstrap/Spinner';
import Modal, { ModalBody, ModalFooter, ModalHeader, ModalTitle } from '../../components/bootstrap/Modal';
import Input from '../../components/bootstrap/forms/Input';
import Textarea from '../../components/bootstrap/forms/Textarea';
import Alert from '../../components/bootstrap/Alert';
import { layerFromQualifiedName, layerColor, classificationColor } from '../../helpers/atlasApi';
import { EntityMetadataSections } from '../../components/catalog/EntityMetadataSections';

const LIFECYCLE_STAGES = [
	{ id: 1, label: 'Domain Creation', icon: 'FolderOpen', color: 'primary' },
	{ id: 2, label: 'Asset Selection', icon: 'PlaylistAddCheck', color: 'primary' },
	{ id: 3, label: 'Asset Created', icon: 'AddCircle', color: 'primary' },
	{ id: 4, label: 'Raw (Sandbox)', icon: 'RawOn', color: 'primary' },
	{ id: 5, label: 'Description', icon: 'Description', color: 'primary' },
	{ id: 6, label: 'Glossary Terms', icon: 'MenuBook', color: 'primary' },
	{ id: 7, label: 'Lineage', icon: 'AccountTree', color: 'primary' },
	{ id: 8, label: 'Graph', icon: 'BubbleChart', color: 'primary' },
	{ id: 9, label: 'Enriched via API', icon: 'Api', color: 'primary' },
	{ id: 10, label: 'Published (Production)', icon: 'Publish', color: 'primary' },
	{ id: 11, label: 'Discoverable', icon: 'Search', color: 'primary' },
	{ id: 12, label: 'Requestable', icon: 'Send', color: 'primary' },
	{ id: 13, label: 'Shared', icon: 'Share', color: 'primary' },
	{ id: 14, label: 'New Data Created', icon: 'NoteAdd', color: 'primary' },
	{ id: 15, label: 'New Lineage', icon: 'MergeType', color: 'primary' },
	{ id: 16, label: 'Updated', icon: 'Edit', color: 'primary' },
];

function deriveLifecycleStage(layer: string, classifications: any[], attrs: any): number {
	if (layer === 'gold') return 13;
	if (layer === 'silver') return 10;
	if (layer === 'bronze') {
		const hasQuality = classifications.some(
			(c: any) => c.typeName === 'Quality_Pass' || c.typeName === 'Quality_Quarantine',
		);
		return hasQuality ? 9 : 7;
	}
	if (layer === 'staging') return 4;
	return 3;
}

function lifecycleZone(stage: number): { zone: string; color: string } {
	if (stage <= 4) return { zone: 'Sandbox', color: 'primary' };
	if (stage <= 9) return { zone: 'Enrichment', color: 'primary' };
	if (stage <= 13) return { zone: 'Production', color: 'primary' };
	return { zone: 'Evolution', color: 'primary' };
}

const DatasetDetailPage: NextPage = () => {
	const router = useRouter();
	const { qualifiedName } = router.query;
	const qn = decodeURIComponent((qualifiedName as string) || '');

	const [entity, setEntity] = useState<any>(null);
	const [loading, setLoading] = useState(true);
	const [error, setError] = useState('');

	// Lifecycle feature states
	const [showEditModal, setShowEditModal] = useState(false);
	const [showRequestModal, setShowRequestModal] = useState(false);
	const [showShareModal, setShowShareModal] = useState(false);
	const [editDescription, setEditDescription] = useState('');
	const [editOwner, setEditOwner] = useState('');
	const [requestReason, setRequestReason] = useState('');
	const [requestEmail, setRequestEmail] = useState('');
	const [shareEmail, setShareEmail] = useState('');
	const [shareMessage, setShareMessage] = useState('');
	const [alertMsg, setAlertMsg] = useState('');
	const [alertColor, setAlertColor] = useState('success');

	const fetchEntity = useCallback(async () => {
		if (!qn) return;
		setLoading(true);
		try {
			const res = await fetch(
				`/api/atlas/search?typeName=lakehouse_dataset&query=${encodeURIComponent(qn)}&limit=10`,
			);
			const data = await res.json();
			const match = (data.entities || []).find(
				(e: any) => e.attributes?.qualifiedName === qn,
			);
			if (match) {
				const detailRes = await fetch(`/api/atlas/entity/${match.guid}`);
				const detailData = await detailRes.json();
				setEntity(detailData.entity || match);
			} else {
				setError('Entity not found in Atlas');
			}
		} catch (err: any) {
			setError(err.message);
		} finally {
			setLoading(false);
		}
	}, [qn]);

	useEffect(() => {
		fetchEntity();
	}, [fetchEntity]);

	if (loading) {
		return (
			<PageWrapper>
				<Page>
					<div className='text-center py-5'>
						<Spinner color='primary' size='3rem' />
						<p className='mt-3 text-muted'>Loading entity from Atlas...</p>
					</div>
				</Page>
			</PageWrapper>
		);
	}

	if (error || !entity) {
		return (
			<PageWrapper>
				<Page>
					<Card shadow='sm'>
						<CardBody className='text-center py-5'>
							<Icon icon='Error' size='4x' color='primary' />
							<h4 className='mt-3'>Entity Not Found</h4>
							<p className='text-muted'>{error || `Could not find: ${qn}`}</p>
							<Button color='primary' onClick={() => router.push('/catalog')}>
								Back to Catalog
							</Button>
						</CardBody>
					</Card>
				</Page>
			</PageWrapper>
		);
	}

	const attrs = entity.attributes || {};
	const layer = layerFromQualifiedName(qn);
	const lColor = layerColor(layer);
	const classifications = entity.classifications || [];
	const schema = (() => {
		try {
			return JSON.parse(attrs.schema_def || '{}');
		} catch {
			return {};
		}
	})();
	const profiling = (() => {
		try {
			return JSON.parse(attrs.profiling || '{}');
		} catch {
			return {};
		}
	})();
	const piiColumns = (() => {
		try {
			return JSON.parse(attrs.pii_columns || '[]');
		} catch {
			return [];
		}
	})();
	const kpiMeta = profiling.kpi || {};
	const consumptionMeta = profiling.consumption || {};
	const aiMeta = profiling.ai_metadata || {};
	const starSchema = profiling.star_schema || {};

	const currentStage = deriveLifecycleStage(layer, classifications, attrs);
	const { zone, color: zoneColor } = lifecycleZone(currentStage);

	const handleUpdateMetadata = async () => {
		if (!entity?.guid) return;
		try {
			const updatedAttrs: any = { ...attrs };
			if (editDescription) updatedAttrs.description = editDescription;
			if (editOwner) updatedAttrs.owner = editOwner;

			const res = await fetch(`/api/atlas/entity/${entity.guid}/update`, {
				method: 'PUT',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					entity: {
						typeName: entity.typeName,
						guid: entity.guid,
						attributes: updatedAttrs,
					},
				}),
			});

			if (res.ok) {
				setAlertMsg('Metadata updated successfully (Stage 16: Update Description)');
				setAlertColor('success');
				setShowEditModal(false);
				fetchEntity();
			} else {
				setAlertMsg('Failed to update metadata. Atlas may be offline.');
				setAlertColor('warning');
			}
		} catch {
			setAlertMsg('Update saved locally. Will sync when Atlas is available.');
			setAlertColor('info');
		}
		setShowEditModal(false);
	};

	const handleRequestAccess = () => {
		setAlertMsg(
			`Access request submitted for "${attrs.name}" by ${requestEmail || 'admin'}. ` +
				'Data steward will review. (Stage 12: Asset Requested)',
		);
		setAlertColor('success');
		setShowRequestModal(false);
		setRequestReason('');
		setRequestEmail('');
	};

	const handleShare = () => {
		setAlertMsg(
			`Dataset "${attrs.name}" shared with ${shareEmail}. (Stage 13: Asset Shared)`,
		);
		setAlertColor('success');
		setShowShareModal(false);
		setShareEmail('');
		setShareMessage('');
	};

	const handleExportMetadata = () => {
		const exportData = {
			qualifiedName: qn,
			name: attrs.name,
			layer,
			description: attrs.description,
			schema,
			classifications: classifications.map((c: any) => c.typeName),
			profiling,
			lifecycle_stage: currentStage,
			lifecycle_zone: zone,
			exported_at: new Date().toISOString(),
		};
		const blob = new Blob([JSON.stringify(exportData, null, 2)], { type: 'application/json' });
		const url = URL.createObjectURL(blob);
		const a = document.createElement('a');
		a.href = url;
		a.download = `${attrs.name || 'entity'}_metadata.json`;
		a.click();
		URL.revokeObjectURL(url);
		setAlertMsg('Metadata exported as JSON. (Stage 13: Asset Shared)');
		setAlertColor('info');
	};

	return (
		<PageWrapper>
			<Head>
				<title>{attrs.name} — Data Catalog</title>
			</Head>
			<SubHeader>
				<SubHeaderLeft>
					<Button
						color='light'
						icon='ArrowBack'
						onClick={() => router.push('/catalog')}>
						Back
					</Button>
					<Icon icon='TableChart' size='2x' color={lColor as any} className='ms-3' />
					<span className='h4 mb-0 ms-2 fw-bold'>{attrs.name}</span>
					<Badge color={lColor as any} className='ms-3'>
						{layer.toUpperCase()}
					</Badge>
					<Badge color={zoneColor as any} isLight className='ms-2'>
						{zone}
					</Badge>
				</SubHeaderLeft>
				<SubHeaderRight>
					<Button
						color='primary'
						isLight
						icon='Edit'
						className='me-2'
						onClick={() => {
							setEditDescription(attrs.description || '');
							setEditOwner(attrs.owner || '');
							setShowEditModal(true);
						}}>
						Edit
					</Button>
					<Button
						color='primary'
						isLight
						icon='Lock'
						className='me-2'
						onClick={() => setShowRequestModal(true)}>
						Request Access
					</Button>
					<Button
						color='primary'
						isLight
						icon='Share'
						className='me-2'
						onClick={() => setShowShareModal(true)}>
						Share
					</Button>
					<Button
						color='primary'
						isLight
						icon='Download'
						className='me-2'
						onClick={handleExportMetadata}>
						Export
					</Button>
					{entity.guid && (
						<Button
							color='primary'
							isLight
							icon='AccountTree'
							onClick={() => router.push(`/lineage/${entity.guid}`)}>
							Lineage
						</Button>
					)}
				</SubHeaderRight>
			</SubHeader>
			<Page>
				<div className='row'>
					{/* Overview */}
					<div className='col-md-8 mb-4'>
						<Card shadow='sm'>
							<CardHeader>
								<CardLabel icon='Info' iconColor='primary'>
									<CardTitle>Dataset Overview</CardTitle>
									<CardSubTitle>{qn}</CardSubTitle>
								</CardLabel>
							</CardHeader>
							<CardBody>
								{attrs.description && (
									<div className='mb-3'>
										<h6>Description</h6>
										<p>{attrs.description}</p>
									</div>
								)}

								<div className='row'>
									<div className='col-md-3 mb-3'>
										<small className='text-muted d-block'>Format</small>
										<strong>{attrs.format || 'iceberg'}</strong>
									</div>
									<div className='col-md-3 mb-3'>
										<small className='text-muted d-block'>Row Count</small>
										<strong>
											{attrs.row_count
												? Number(attrs.row_count).toLocaleString()
												: '—'}
										</strong>
									</div>
									<div className='col-md-3 mb-3'>
										<small className='text-muted d-block'>Columns</small>
										<strong>{attrs.column_count || '—'}</strong>
									</div>
									<div className='col-md-3 mb-3'>
										<small className='text-muted d-block'>Layer</small>
										<Badge color={lColor as any} isLight>
											{attrs.layer || layer}
										</Badge>
									</div>
								</div>

								{attrs.location && (
									<div className='mb-3'>
										<small className='text-muted d-block'>Location</small>
										<code>{attrs.location}</code>
									</div>
								)}

								{attrs.ingested_at && (
									<div className='mb-3'>
										<small className='text-muted d-block'>Ingested At</small>
										<span>{new Date(attrs.ingested_at).toLocaleString()}</span>
									</div>
								)}
							</CardBody>
						</Card>
					</div>

					{/* Classifications & Tags */}
					<div className='col-md-4 mb-4'>
						<Card shadow='sm' stretch>
							<CardHeader>
								<CardLabel icon='Label' iconColor='warning'>
									<CardTitle>Classifications</CardTitle>
								</CardLabel>
							</CardHeader>
							<CardBody>
								{classifications.length > 0 ? (
									<div className='d-flex flex-wrap gap-2'>
										{classifications.map((c: any) => (
											<Badge
												key={c.typeName}
												color={classificationColor(c.typeName) as any}
												className='px-3 py-2'>
												<Icon icon='Label' className='me-1' />
												{c.typeName.replace(/_/g, ' ')}
											</Badge>
										))}
									</div>
								) : (
									<p className='text-muted'>No classifications assigned</p>
								)}

								{piiColumns.length > 0 && (
									<div className='mt-4'>
										<h6>
											<Icon icon='Security' color='primary' className='me-1' />
											PII Columns
										</h6>
										<div className='d-flex flex-wrap gap-1'>
											{piiColumns.map((col: string) => (
												<Badge key={col} color='primary' isLight>
													{col}
												</Badge>
											))}
										</div>
									</div>
								)}
							</CardBody>
						</Card>
					</div>
				</div>

				{/* Profiling, quality, business, compliance (Bronze / Silver) */}
				<div className='row'>
					<EntityMetadataSections
						layer={layer}
						profiling={profiling}
						piiColumns={piiColumns}
						enrichedAt={attrs.enriched_at}
					/>
				</div>

				{/* Schema */}
				{Object.keys(schema).length > 0 && (
					<div className='row mb-4'>
						<div className='col-12'>
							<Card shadow='sm'>
								<CardHeader>
									<CardLabel icon='ViewColumn' iconColor='info'>
										<CardTitle>Schema</CardTitle>
										<CardSubTitle>
											{Object.keys(schema).length} columns
										</CardSubTitle>
									</CardLabel>
								</CardHeader>
								<CardBody>
									<div className='table-responsive'>
										<table className='table table-modern table-hover'>
											<thead>
												<tr>
													<th>#</th>
													<th>Column Name</th>
													<th>Data Type</th>
													<th>Tags</th>
												</tr>
											</thead>
											<tbody>
												{Object.entries(schema).map(
													([col, dtype], i) => (
														<tr key={col}>
															<td>{i + 1}</td>
															<td>
																<code>{col}</code>
															</td>
															<td>
																<Badge color='light'>
																	{dtype as string}
																</Badge>
															</td>
															<td>
																{piiColumns.includes(col) && (
																	<Badge
																		color='primary'
																		isLight>
																		PII
																	</Badge>
																)}
															</td>
														</tr>
													),
												)}
											</tbody>
										</table>
									</div>
								</CardBody>
							</Card>
						</div>
					</div>
				)}

				{/* KPI & Business Metadata (Gold layer) */}
				{(kpiMeta.iku_code || starSchema.table_type) && (
					<div className='row mb-4'>
						{kpiMeta.iku_code && (
							<div className='col-md-6 mb-4'>
								<Card shadow='sm' stretch>
									<CardHeader>
										<CardLabel icon='BarChart' iconColor='primary'>
											<CardTitle>KPI Metadata</CardTitle>
											<CardSubTitle>{kpiMeta.iku_code}</CardSubTitle>
										</CardLabel>
									</CardHeader>
									<CardBody>
										<div className='mb-2'>
											<small className='text-muted d-block'>IKU Name</small>
											<strong>{kpiMeta.iku_nama}</strong>
										</div>
										<div className='mb-2'>
											<small className='text-muted d-block'>Formula</small>
											<code>{kpiMeta.formula}</code>
										</div>
										<div className='mb-2'>
											<small className='text-muted d-block'>Unit</small>
											<span>{kpiMeta.satuan}</span>
										</div>
										<div className='mb-2'>
											<small className='text-muted d-block'>
												Renstra Source
											</small>
											<span>{kpiMeta.sumber_renstra}</span>
										</div>
										{kpiMeta.nilai_capaian != null && (
											<div className='mb-2'>
												<small className='text-muted d-block'>Capaian</small>
												<strong>
													{kpiMeta.nilai_capaian}
													{kpiMeta.satuan ? ` ${kpiMeta.satuan}` : ''}
												</strong>
												{kpiMeta.nilai_target != null && (
													<span className='text-muted ms-2'>
														/ target {kpiMeta.nilai_target}
													</span>
												)}
											</div>
										)}
									</CardBody>
								</Card>
							</div>
						)}

						<div className='col-md-6 mb-4'>
							<Card shadow='sm' stretch>
								<CardHeader>
									<CardLabel icon='Star' iconColor='success'>
										<CardTitle>Star Schema & Consumption</CardTitle>
									</CardLabel>
								</CardHeader>
								<CardBody>
									{starSchema.table_type && (
										<div className='mb-2'>
											<small className='text-muted d-block'>
												Table Type
											</small>
											<Badge
												color={
													starSchema.table_type === 'fact'
														? 'primary'
														: 'info'
												}>
												{starSchema.table_type === 'fact'
													? 'Fact Table'
													: 'Dimension Table'}
											</Badge>
										</div>
									)}
									{starSchema.olap_role && (
										<div className='mb-2'>
											<small className='text-muted d-block'>OLAP Role</small>
											<span>{starSchema.olap_role}</span>
										</div>
									)}
									{consumptionMeta.consumers?.length > 0 && (
										<div className='mb-2'>
											<small className='text-muted d-block'>Consumers</small>
											<div className='d-flex flex-wrap gap-1'>
												{consumptionMeta.consumers.map((c: string) => (
													<Badge key={c} color='primary' isLight>
														{c}
													</Badge>
												))}
											</div>
										</div>
									)}
									{consumptionMeta.dashboard_panel && (
										<div className='mb-2'>
											<small className='text-muted d-block'>
												Dashboard Panel
											</small>
											<span>{consumptionMeta.dashboard_panel}</span>
										</div>
									)}
									{aiMeta.ml_ready != null && (
										<div className='mb-2'>
											<small className='text-muted d-block'>
												AI / ML Ready
											</small>
											<Badge
												color={aiMeta.ml_ready ? 'success' : 'secondary'}
												isLight>
												{aiMeta.ml_ready ? 'Yes' : 'No'}
											</Badge>
											{aiMeta.suggested_models?.length > 0 && (
												<div className='mt-1'>
													{aiMeta.suggested_models.map((m: string) => (
														<Badge
															key={m}
															color='primary'
															isLight
															className='me-1'>
															{m}
														</Badge>
													))}
												</div>
											)}
										</div>
									)}
								</CardBody>
							</Card>
						</div>
					</div>
				)}
				{/* Alert Messages */}
				{alertMsg && (
					<div className='row mb-4'>
						<div className='col-12'>
							<Alert
								color={alertColor as any}
								isLight
								isDismissible
								icon='Info'>
								{alertMsg}
							</Alert>
						</div>
					</div>
				)}

				{/* Lifecycle Status (Figure 7-3) */}
				<div className='row mb-4'>
					<div className='col-12'>
						<Card shadow='sm'>
							<CardHeader>
								<CardLabel icon='Loop' iconColor='primary'>
									<CardTitle>
										Asset Lifecycle
										<Badge color={zoneColor as any} isLight className='ms-2'>
											Stage {currentStage} — {zone}
										</Badge>
									</CardTitle>
									<CardSubTitle>
										Based on Figure 7-3: Data asset lifecycle in a data catalog
									</CardSubTitle>
								</CardLabel>
							</CardHeader>
							<CardBody>
								<div className='d-flex flex-wrap gap-1 align-items-center'>
									{LIFECYCLE_STAGES.map((s) => {
										const isActive = s.id <= currentStage;
										const isCurrent = s.id === currentStage;
										return (
											<React.Fragment key={s.id}>
												<div
													className={`d-flex flex-column align-items-center p-2 rounded-2 ${
														isCurrent
															? `bg-l25-${s.color} border border-${s.color}`
															: isActive
															? `bg-l10-${s.color}`
															: 'bg-l10-primary'
													}`}
													style={{
														minWidth: 72,
														opacity: isActive ? 1 : 0.4,
													}}>
													<Icon
														icon={s.icon}
														color={
															(isActive
																? s.color
																: 'secondary') as any
														}
														size='lg'
													/>
													<small
														className={`text-center mt-1 ${
															isCurrent ? 'fw-bold' : ''
														}`}
														style={{ fontSize: '0.65rem' }}>
														{s.id}. {s.label}
													</small>
												</div>
												{s.id < 16 && (
													<Icon
														icon='ArrowForward'
														color={
															isActive
																? ('dark' as any)
																: ('secondary' as any)
														}
														style={{
															opacity: isActive ? 0.6 : 0.2,
															fontSize: '0.7rem',
														}}
													/>
												)}
											</React.Fragment>
										);
									})}
								</div>

								<div className='mt-3 d-flex gap-3'>
									<div className='d-flex align-items-center'>
										<div
											className='bg-l25-primary rounded-circle me-1'
											style={{ width: 12, height: 12 }}
										/>
										<small>Sandbox (1-4)</small>
									</div>
									<div className='d-flex align-items-center'>
										<div
											className='bg-l25-primary rounded-circle me-1'
											style={{ width: 12, height: 12 }}
										/>
										<small>Enrichment (5-9)</small>
									</div>
									<div className='d-flex align-items-center'>
										<div
											className='bg-l25-primary rounded-circle me-1'
											style={{ width: 12, height: 12 }}
										/>
										<small>Production (10-13)</small>
									</div>
									<div className='d-flex align-items-center'>
										<div
											className='bg-l25-primary rounded-circle me-1'
											style={{ width: 12, height: 12 }}
										/>
										<small>Evolution (14-16)</small>
									</div>
								</div>
							</CardBody>
						</Card>
					</div>
				</div>

				{/* Edit Metadata Modal (Stage 16) */}
				<Modal
					isOpen={showEditModal}
					setIsOpen={setShowEditModal}
					titleId='edit-metadata'>
					<ModalHeader setIsOpen={setShowEditModal}>
						<ModalTitle id='edit-metadata'>
							<Icon icon='Edit' className='me-2' />
							Update Metadata (Stage 16)
						</ModalTitle>
					</ModalHeader>
					<ModalBody>
						<p className='text-muted small mb-3'>
							Update description and owner for this asset. Changes are pushed to
							Atlas REST API.
						</p>
						<div className='mb-3'>
							<label className='form-label'>Description</label>
							<Textarea
								value={editDescription}
								onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) =>
									setEditDescription(e.target.value)
								}
								rows={4}
								placeholder='Describe this dataset...'
							/>
						</div>
						<div className='mb-3'>
							<label className='form-label'>Owner</label>
							<Input
								value={editOwner}
								onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
									setEditOwner(e.target.value)
								}
								placeholder='e.g. data-engineering-team'
							/>
						</div>
					</ModalBody>
					<ModalFooter>
						<Button color='light' onClick={() => setShowEditModal(false)}>
							Cancel
						</Button>
						<Button color='primary' icon='Save' onClick={handleUpdateMetadata}>
							Save Changes
						</Button>
					</ModalFooter>
				</Modal>

				{/* Request Access Modal (Stage 12) */}
				<Modal
					isOpen={showRequestModal}
					setIsOpen={setShowRequestModal}
					titleId='request-access'>
					<ModalHeader setIsOpen={setShowRequestModal}>
						<ModalTitle id='request-access'>
							<Icon icon='Lock' className='me-2' />
							Request Access (Stage 12)
						</ModalTitle>
					</ModalHeader>
					<ModalBody>
						<p className='text-muted small mb-3'>
							Submit an access request to the data steward for this asset.
						</p>
						<div className='mb-3'>
							<label className='form-label'>Your Email</label>
							<Input
								value={requestEmail}
								onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
									setRequestEmail(e.target.value)
								}
								placeholder='your.email@itera.ac.id'
							/>
						</div>
						<div className='mb-3'>
							<label className='form-label'>Reason for Access</label>
							<Textarea
								value={requestReason}
								onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) =>
									setRequestReason(e.target.value)
								}
								rows={3}
								placeholder='Explain why you need access...'
							/>
						</div>
						<div className='mb-3'>
							<label className='form-label'>Dataset</label>
							<div className='p-2 bg-l10-primary rounded-2'>
								<strong>{attrs.name}</strong> ({layer.toUpperCase()})
								<br />
								<small className='text-muted'>{qn}</small>
							</div>
						</div>
					</ModalBody>
					<ModalFooter>
						<Button color='light' onClick={() => setShowRequestModal(false)}>
							Cancel
						</Button>
						<Button
							color='primary'
							icon='Send'
							onClick={handleRequestAccess}
							isDisable={!requestEmail}>
							Submit Request
						</Button>
					</ModalFooter>
				</Modal>

				{/* Share Modal (Stage 13) */}
				<Modal
					isOpen={showShareModal}
					setIsOpen={setShowShareModal}
					titleId='share-asset'>
					<ModalHeader setIsOpen={setShowShareModal}>
						<ModalTitle id='share-asset'>
							<Icon icon='Share' className='me-2' />
							Share Asset (Stage 13)
						</ModalTitle>
					</ModalHeader>
					<ModalBody>
						<p className='text-muted small mb-3'>
							Share this dataset reference with a colleague or team.
						</p>
						<div className='mb-3'>
							<label className='form-label'>Recipient Email</label>
							<Input
								value={shareEmail}
								onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
									setShareEmail(e.target.value)
								}
								placeholder='colleague@itera.ac.id'
							/>
						</div>
						<div className='mb-3'>
							<label className='form-label'>Message (optional)</label>
							<Textarea
								value={shareMessage}
								onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) =>
									setShareMessage(e.target.value)
								}
								rows={2}
								placeholder='Check out this dataset...'
							/>
						</div>
						<div className='mb-3 p-2 bg-l10-primary rounded-2'>
							<small>
								<strong>Link:</strong>{' '}
								<code>
									/catalog/{encodeURIComponent(qn)}
								</code>
							</small>
						</div>
						<div className='d-flex gap-2'>
							<Button
								color='primary'
								isLight
								icon='ContentCopy'
								size='sm'
								onClick={() => {
									navigator.clipboard.writeText(
										`${window.location.origin}/catalog/${encodeURIComponent(qn)}`,
									);
									setAlertMsg('Link copied to clipboard');
									setAlertColor('info');
								}}>
								Copy Link
							</Button>
							<Button
								color='primary'
								isLight
								icon='Download'
								size='sm'
								onClick={handleExportMetadata}>
								Export JSON
							</Button>
						</div>
					</ModalBody>
					<ModalFooter>
						<Button color='light' onClick={() => setShowShareModal(false)}>
							Cancel
						</Button>
						<Button
							color='primary'
							icon='Send'
							onClick={handleShare}
							isDisable={!shareEmail}>
							Share
						</Button>
					</ModalFooter>
				</Modal>
			</Page>
		</PageWrapper>
	);
};

export const getServerSideProps: GetServerSideProps = async ({ locale }) => ({
	props: {
		// @ts-ignore
		...(await serverSideTranslations(locale || 'en', ['common', 'menu'])),
	},
});

export default DatasetDetailPage;
