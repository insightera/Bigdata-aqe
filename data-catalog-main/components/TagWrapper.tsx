import React, { ElementType, forwardRef, HTMLAttributes, ReactNode } from 'react';

interface ITagWrapper extends Record<string, any>, HTMLAttributes<HTMLElement> {
	children: ReactNode;
	tag: ElementType;
}
const TagWrapper = forwardRef<HTMLDivElement | HTMLAnchorElement | HTMLFormElement, ITagWrapper>(
	({ tag: Tag = 'div', children, ...props }, ref) => {
		return (
			// eslint-disable-next-line react/jsx-props-no-spreading
			<Tag ref={ref} {...props}>
				{children}
			</Tag>
		);
	},
);
TagWrapper.displayName = 'TagWrapper';

export default TagWrapper;
