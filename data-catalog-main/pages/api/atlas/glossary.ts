import type { NextApiRequest, NextApiResponse } from 'next';
import { atlasRequest } from '../../../helpers/atlasApi';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
	if (req.method === 'GET') {
		try {
			const result = await atlasRequest('/api/atlas/v2/glossary');
			res.status(200).json(result);
		} catch (err: any) {
			res.status(502).json({ error: err.message });
		}
	} else if (req.method === 'POST') {
		try {
			const result = await atlasRequest('/api/atlas/v2/glossary', {
				method: 'POST',
				body: JSON.stringify(req.body),
			});
			res.status(200).json(result);
		} catch (err: any) {
			res.status(502).json({ error: err.message });
		}
	} else {
		res.status(405).json({ error: 'Method not allowed' });
	}
}
