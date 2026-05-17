import type { NextApiRequest, NextApiResponse } from 'next';
import { buildMetadataQualityReport } from '../../../helpers/metadataQualityEvaluator';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
	if (req.method !== 'GET') {
		res.setHeader('Allow', 'GET');
		return res.status(405).json({ error: 'Method not allowed' });
	}

	try {
		const report = await buildMetadataQualityReport();
		res.status(200).json(report);
	} catch (err: unknown) {
		const message = err instanceof Error ? err.message : 'Metadata quality evaluation failed';
		res.status(502).json({ error: message });
	}
}
