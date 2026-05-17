import React, { forwardRef, ReactNode } from 'react';
import classNames from 'classnames';

export interface IPageProps {
	children: ReactNode;
	container?: boolean | 'sm' | 'md' | 'lg' | 'xl' | 'xxl' | 'fluid';
	className?: string;
}
const Page = forwardRef<HTMLDivElement, IPageProps>(
	({ children, className, container = 'xxl', ...props }, ref) => {
		return (
			<div
				ref={ref}
				className={classNames('page', className, {
					[`container${typeof container === 'string' ? `-${container}` : ''}`]: container,
				})}
				{...props}>
				{children}
			</div>
		);
	},
);
Page.displayName = 'Page';

export default Page;
