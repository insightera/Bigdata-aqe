import type { NextApiRequest, NextApiResponse } from 'next';
import { getLineage } from '../../../../helpers/atlasApi';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
	try {
		const { guid, depth = '5', direction = 'BOTH' } = req.query;
		const result = await getLineage(
			guid as string,
			Number(depth),
			direction as 'BOTH' | 'INPUT' | 'OUTPUT',
		);
		res.status(200).json(result);
	} catch (err: any) {
		res.status(502).json({ error: err.message });
	}
}
