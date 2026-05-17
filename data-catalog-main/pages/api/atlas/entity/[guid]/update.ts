import type { NextApiRequest, NextApiResponse } from 'next';
import { atlasRequest } from '../../../../../helpers/atlasApi';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
	if (req.method !== 'PUT') {
		return res.status(405).json({ error: 'Method not allowed' });
	}
	const { guid } = req.query;
	try {
		const result = await atlasRequest(`/api/atlas/v2/entity/guid/${guid}`, {
			method: 'PUT',
			body: JSON.stringify(req.body),
		});
		res.status(200).json(result);
	} catch (err: any) {
		res.status(502).json({ error: err.message });
	}
}
