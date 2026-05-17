import type { NextApiRequest, NextApiResponse } from 'next';
import { getMetrics } from '../../../helpers/atlasApi';

export default async function handler(_req: NextApiRequest, res: NextApiResponse) {
	try {
		const result = await getMetrics();
		res.status(200).json(result);
	} catch (err: any) {
		res.status(502).json({ error: err.message });
	}
}
