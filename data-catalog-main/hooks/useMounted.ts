import { useEffect, useState } from 'react';

const useMounted = () => {
	const [mounted, setMounted] = useState<boolean>(false);

	useEffect(() => {
		// eslint-disable-next-line react-hooks/set-state-in-effect
		setMounted(true);

		return () => setMounted(false);
	}, []);

	return { mounted };
};

export default useMounted;
