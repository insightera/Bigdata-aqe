import { useEffect, useState } from 'react';
import useMounted from './useMounted';

const useRoot = () => {
	const { mounted } = useMounted();

	const [root, setRoot] = useState<any>(null);
	useEffect(() => {
		if (mounted) {
			// eslint-disable-next-line react-hooks/set-state-in-effect
			setRoot(document.documentElement);
		}
	}, [mounted]);

	return root;
};

export default useRoot;
