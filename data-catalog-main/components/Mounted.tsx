import React, { FC, ReactNode } from 'react';
import useMounted from '../hooks/useMounted';

interface IMountedProps {
	children: ReactNode;
}
const Mounted: FC<IMountedProps> = ({ children }) => {
	const { mounted } = useMounted();

	if (mounted) return <>{children}</>;
	return null;
};

export default Mounted;
