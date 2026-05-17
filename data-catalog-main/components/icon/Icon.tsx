import React, { forwardRef, HTMLAttributes, memo, ReactNode } from 'react';
import classNames from 'classnames';
import * as SvgIcon from './svg-icons';
import * as Material from './material-icons';
import pascalcase from 'pascalcase';
import { TColor } from '../../type/color-type';
import { TIcons, TIconsSize } from '../../type/icons-type';

interface IRefWrapperProps extends Record<string, any> {
	children: ReactNode;
}
// @ts-ignore

const RefWrapper = forwardRef<HTMLSpanElement, IRefWrapperProps>(({ children }, ref) => {
	if (ref) {
		return (
			<span ref={ref} data-only-ref='true'>
				{children}
			</span>
		);
	}
	return children;
});
RefWrapper.displayName = 'RefWrapper';

interface IIconProps extends HTMLAttributes<HTMLSpanElement> {
	icon?: TIcons;
	className?: string;
	color?: TColor;
	size?: TIconsSize;
	forceFamily?: null | 'custom' | 'material';
}
const Icon = forwardRef<HTMLSpanElement, IIconProps>(
	({ icon, className, color, size, forceFamily, ...props }, ref) => {
		const IconName = pascalcase(icon);

		// @ts-ignore

		const SvgIconWrapper = SvgIcon[IconName];
		// @ts-ignore

		const MaterialWrapper = Material[IconName];

		const ClassName = classNames(
			'svg-icon',
			{ [`svg-icon-${size}`]: size, [`text-${color}`]: color },
			className,
		);

		const isForceCustom = forceFamily === 'custom';
		const isForceMaterial = forceFamily === 'material';

		if (isForceCustom || (!isForceMaterial && typeof SvgIconWrapper === 'function')) {
			return (
				<RefWrapper ref={ref}>
					<SvgIconWrapper
						data-name={`SvgIcon--${IconName}`}
						className={classNames('svg-icon--custom', ClassName)}
						// eslint-disable-next-line react/jsx-props-no-spreading
						{...props}
					/>
				</RefWrapper>
			);
		}
		if (isForceMaterial || (!isForceCustom && typeof MaterialWrapper === 'function')) {
			return (
				<RefWrapper ref={ref}>
					<MaterialWrapper
						data-name={`Material--${icon}`}
						className={classNames('svg-icon--material', ClassName)}
						// eslint-disable-next-line react/jsx-props-no-spreading
						{...props}
					/>
				</RefWrapper>
			);
		}
		return null;
	},
);
Icon.displayName = 'Icon';

export default memo(Icon);
