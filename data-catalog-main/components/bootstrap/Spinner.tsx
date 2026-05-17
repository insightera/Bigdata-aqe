import React, { forwardRef, ReactNode } from 'react';
import classNames from 'classnames';
import { createUseStyles } from 'react-jss';
import TagWrapper from '../TagWrapper';
import { TColor } from '../../type/color-type';
import useMounted from '../../hooks/useMounted';

const useStyles = createUseStyles(
	{
		// stylelint-disable-next-line selector-type-no-unknown
		dynamicSize: (props) => ({
			// @ts-ignore
			height: props.size,
			// @ts-ignore
			width: props.size,
		}),
	},
	{ link: true },
);

interface ISpinnerProps {
	children?: ReactNode;
	tag?: 'div' | 'span';
	color?: TColor | null;
	isGrow?: boolean;
	isSmall?: boolean;
	size?: string | number | null;
	inButton?: boolean | 'onlyIcon';
	className?: string;
}
const Spinner = forwardRef<HTMLDivElement, ISpinnerProps>(
	(
		{
			tag = 'div',
			color,
			isGrow,
			isSmall,
			size,
			children = 'Loading...',
			inButton,
			className,
			...props
		},
		ref,
	) => {
		const { mounted } = useMounted();

		// @ts-ignore
		const classes = useStyles({ size });

		const HIDDEN_TEXT = <span className='visually-hidden'>{children}</span>;
		return (
			<>
				<TagWrapper
					ref={ref}
					tag={inButton ? 'span' : tag}
					className={classNames(
						{ 'spinner-border': !isGrow, 'spinner-grow': isGrow },
						{
							'spinner-border-sm': !isGrow && isSmall,
							'spinner-grow-sm': isGrow && isSmall,
						},
						{ [`text-${color}`]: color },
						{ [classes.dynamicSize]: mounted && size },
						{ 'me-2': inButton !== 'onlyIcon' && !!inButton },
						className,
					)}
					role='status'
					aria-hidden={inButton ? 'true' : null}
					// eslint-disable-next-line react/jsx-props-no-spreading
					{...props}>
					{inButton !== 'onlyIcon' && !!inButton ? HIDDEN_TEXT : ''}
				</TagWrapper>
				{inButton === 'onlyIcon' ? HIDDEN_TEXT : null}
			</>
		);
	},
);
Spinner.displayName = 'Spinner';

export default Spinner;
