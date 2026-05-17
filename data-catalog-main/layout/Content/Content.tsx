import React, { FC, ReactNode } from 'react';

interface IContent {
	children: ReactNode;
}
const Content: FC<IContent> = ({ children }) => {
	return <main className='content'>{children}</main>;
};

export default Content;
