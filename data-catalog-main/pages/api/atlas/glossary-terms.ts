import type { NextApiRequest, NextApiResponse } from 'next';
import { buildGlossaryFromAtlas } from '../../../helpers/glossaryFromMetadata';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
	if (req.method !== 'GET') {
		res.setHeader('Allow', 'GET');
		return res.status(405).json({ error: 'Method not allowed' });
	}

	try {
		const limit = Math.min(Number(req.query.limit) || 500, 1000);
		const payload = await buildGlossaryFromAtlas(limit);
		res.status(200).json(payload);
	} catch (err: unknown) {
		const message = err instanceof Error ? err.message : 'Glossary build failed';
		res.status(502).json({ error: message });
	}
}
