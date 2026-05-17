import type { NextApiRequest, NextApiResponse } from 'next';
import { atlasRequest } from '../../../../../helpers/atlasApi';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
	const { glossaryGuid } = req.query;
	try {
		const result = await atlasRequest(
			`/api/atlas/v2/glossary/${glossaryGuid}/terms`,
		);
		res.status(200).json(result);
	} catch (err: any) {
		res.status(502).json({ error: err.message });
	}
}
