import type { NextApiRequest, NextApiResponse } from 'next';
import { getEntity } from '../../../../helpers/atlasApi';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
	try {
		const { guid } = req.query;
		const result = await getEntity(guid as string);
		res.status(200).json(result);
	} catch (err: any) {
		res.status(502).json({ error: err.message });
	}
}
