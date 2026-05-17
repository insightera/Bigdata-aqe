import type { NextApiRequest, NextApiResponse } from 'next';
import { searchEntities } from '../../../helpers/atlasApi';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
	try {
		const {
			typeName = 'lakehouse_dataset',
			query,
			classification,
			limit = '50',
			offset = '0',
		} = req.query;

		const result = await searchEntities(
			typeName as string,
			query as string | undefined,
			classification as string | undefined,
			Number(limit),
			Number(offset),
		);

		res.status(200).json(result);
	} catch (err: any) {
		res.status(502).json({ error: err.message });
	}
}
