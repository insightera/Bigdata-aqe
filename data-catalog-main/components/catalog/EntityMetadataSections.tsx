import React from 'react';
import Card, {
	CardBody,
	CardHeader,
	CardLabel,
	CardTitle,
	CardSubTitle,
} from '../bootstrap/Card';
import Badge from '../bootstrap/Badge';
import Icon from '../icon/Icon';
import {
	getColumnProfiling,
	getQualityMeta,
	getBusinessMeta,
	getComplianceMeta,
	getTransformations,
	qualityStatusColor,
	qualityDisplayScore,
	qualityDisplayStatus,
	type ColumnStat,
} from '../../helpers/entityProfiling';

interface Props {
	layer: string;
	profiling: Record<string, unknown>;
	piiColumns: string[];
	enrichedAt?: string;
}

function ColumnProfilingTable({ columns }: { columns: Record<string, ColumnStat> }) {
	const entries = Object.entries(columns);
	if (entries.length === 0) return null;

	return (
		<div className='table-responsive'>
			<table className='table table-modern table-sm mb-0'>
				<thead>
					<tr>
						<th>Column</th>
						<th>Type</th>
						<th>Null %</th>
						<th>Completeness</th>
						<th>Distinct</th>
					</tr>
				</thead>
				<tbody>
					{entries.map(([name, stat]) => (
						<tr key={name}>
							<td>
								<code>{name}</code>
							</td>
							<td>
								<small>{stat.data_type || '—'}</small>
							</td>
							<td>{stat.null_pct != null ? `${stat.null_pct}%` : '—'}</td>
							<td>
								{stat.completeness_pct != null ? (
									<Badge
										color={
											stat.completeness_pct >= 80
												? 'success'
												: stat.completeness_pct >= 60
													? 'warning'
													: 'danger'
										}
										isLight>
										{stat.completeness_pct}%
									</Badge>
								) : (
									'—'
								)}
							</td>
							<td>
								{stat.distinct_count != null
									? stat.distinct_count.toLocaleString()
									: '—'}
							</td>
						</tr>
					))}
				</tbody>
			</table>
		</div>
	);
}

export function EntityMetadataSections({ layer, profiling, piiColumns, enrichedAt }: Props) {
	const columns = getColumnProfiling(profiling);
	const quality = getQualityMeta(profiling);
	const business = getBusinessMeta(profiling);
	const compliance = getComplianceMeta(profiling, piiColumns);
	const transformations = getTransformations(profiling);
	const profiledAt = profiling.profiled_at as string | undefined;
	const hasColumnProfiling = Object.keys(columns).length > 0;

	return (
		<>
			{hasColumnProfiling && (
				<div className='col-12 mb-4'>
					<Card shadow='sm'>
						<CardHeader>
							<CardLabel icon='Analytics' iconColor='info'>
								<CardTitle>Data Profiling</CardTitle>
								<CardSubTitle>
									{layer === 'bronze'
										? 'Statistik kolom dari ingest Bronze (Spark)'
										: 'Statistik kolom setelah transformasi Silver'}
								</CardSubTitle>
							</CardLabel>
							{profiledAt && (
								<small className='text-muted'>Profiled: {profiledAt}</small>
							)}
						</CardHeader>
						<CardBody>
							<ColumnProfilingTable columns={columns} />
						</CardBody>
					</Card>
				</div>
			)}

			{layer === 'silver' && enrichedAt && (
				<div className='col-12 mb-4'>
					<Card shadow='sm' className='bg-l10-success'>
						<CardBody className='py-3'>
							<Icon icon='AutoFixHigh' color='success' className='me-2' />
							<strong>Clean metadata:</strong> schema dan atribut bisnis diperbarui pada{' '}
							{new Date(enrichedAt).toLocaleString()}.
						</CardBody>
					</Card>
				</div>
			)}

			{quality && (
				<div className='col-md-6 mb-4'>
					<Card shadow='sm' stretch>
						<CardHeader>
							<CardLabel icon='Verified' iconColor='success'>
								<CardTitle>Quality Metadata</CardTitle>
							</CardLabel>
						</CardHeader>
						<CardBody>
							<div className='d-flex align-items-center mb-3'>
								<span className='display-6 fw-bold me-3'>
									{qualityDisplayScore(quality)}
								</span>
								<Badge
									color={qualityStatusColor(qualityDisplayStatus(quality))}
									className='px-3 py-2'>
									{qualityDisplayStatus(quality)}
								</Badge>
							</div>
							<ul className='list-unstyled mb-0 small'>
								{quality.silver_completeness != null && (
									<li>
										<strong>Silver completeness:</strong>{' '}
										{quality.silver_completeness}%
									</li>
								)}
								{quality.avg_completeness != null && (
									<li>
										<strong>Avg completeness:</strong> {quality.avg_completeness}%
									</li>
								)}
								{quality.rules_passed != null && quality.rules_total != null && (
									<li>
										<strong>Rules:</strong> {quality.rules_passed}/{quality.rules_total}{' '}
										passed
									</li>
								)}
								{quality.source_status && (
									<li>
										<strong>Source status:</strong> {quality.source_status}
									</li>
								)}
							</ul>
						</CardBody>
					</Card>
				</div>
			)}

			{business && (
				<div className='col-md-6 mb-4'>
					<Card shadow='sm' stretch>
						<CardHeader>
							<CardLabel icon='Business' iconColor='primary'>
								<CardTitle>Business Metadata</CardTitle>
							</CardLabel>
						</CardHeader>
						<CardBody>
							{business.owner && (
								<p className='mb-2'>
									<small className='text-muted d-block'>Owner</small>
									<strong>{business.owner}</strong>
								</p>
							)}
							{business.update_frequency && (
								<p className='mb-2'>
									<small className='text-muted d-block'>Update frequency</small>
									{business.update_frequency}
								</p>
							)}
							{business.iku_relevance && business.iku_relevance.length > 0 && (
								<div className='mb-2'>
									<small className='text-muted d-block'>IKU relevance</small>
									<div className='d-flex flex-wrap gap-1'>
										{business.iku_relevance.map((iku) => (
											<Badge key={iku} color='primary' isLight>
												{iku}
											</Badge>
										))}
									</div>
								</div>
							)}
							{business.glossary_terms && business.glossary_terms.length > 0 && (
								<div>
									<small className='text-muted d-block'>Glossary terms</small>
									<div className='d-flex flex-wrap gap-1'>
										{business.glossary_terms.map((t) => (
											<Badge key={t} color='info' isLight>
												{t}
											</Badge>
										))}
									</div>
								</div>
							)}
						</CardBody>
					</Card>
				</div>
			)}

			{compliance && (
				<div className='col-md-6 mb-4'>
					<Card shadow='sm' stretch>
						<CardHeader>
							<CardLabel icon='Gavel' iconColor='warning'>
								<CardTitle>Compliance</CardTitle>
							</CardLabel>
						</CardHeader>
						<CardBody>
							<ul className='list-unstyled mb-0 small'>
								<li>
									<strong>Contains PII:</strong>{' '}
									{compliance.contains_pii ? 'Yes' : 'No'}
								</li>
								{compliance.data_classification && (
									<li>
										<strong>Classification:</strong>{' '}
										{compliance.data_classification}
									</li>
								)}
								{compliance.retention_policy && (
									<li>
										<strong>Retention:</strong> {compliance.retention_policy}
									</li>
								)}
								{compliance.access_control && (
									<li>
										<strong>Access:</strong> {compliance.access_control}
									</li>
								)}
							</ul>
							{compliance.pii_columns && compliance.pii_columns.length > 0 && (
								<div className='mt-2'>
									<small className='text-muted d-block'>PII columns</small>
									{compliance.pii_columns.map((c) => (
										<Badge key={c} color='danger' isLight className='me-1'>
											{c}
										</Badge>
									))}
								</div>
							)}
						</CardBody>
					</Card>
				</div>
			)}

			{transformations.length > 0 && (
				<div className='col-12 mb-4'>
					<Card shadow='sm'>
						<CardHeader>
							<CardLabel icon='Transform' iconColor='secondary'>
								<CardTitle>Transformations (Bronze → Silver)</CardTitle>
							</CardLabel>
						</CardHeader>
						<CardBody>
							<ul className='mb-0'>
								{transformations.map((t) => (
									<li key={t}>
										<code>{t}</code>
									</li>
								))}
							</ul>
						</CardBody>
					</Card>
				</div>
			)}
		</>
	);
}
