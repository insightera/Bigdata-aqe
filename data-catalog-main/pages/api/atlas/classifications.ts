import type { NextApiRequest, NextApiResponse } from 'next';
import { getClassificationDefs } from '../../../helpers/atlasApi';

export default async function handler(_req: NextApiRequest, res: NextApiResponse) {
	try {
		const result = await getClassificationDefs();
		res.status(200).json(result);
	} catch (err: any) {
		res.status(502).json({ error: err.message });
	}
}
