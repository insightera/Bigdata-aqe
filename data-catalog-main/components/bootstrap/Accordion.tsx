import React, {
	Children,
	cloneElement,
	ElementType,
	forwardRef,
	ReactElement,
	ReactNode,
	useState,
} from 'react';
import classNames from 'classnames';
import Icon from '../icon/Icon';
import TagWrapper from '../TagWrapper';
import Collapse from './Collapse';
import { TColor } from '../../type/color-type';
import { TIcons } from '../../type/icons-type';

type TActiveItemId = string | number | boolean | null;

interface IAccordionItemProps {
	id: string | number;
	icon?: TIcons;
	title: string;
	children: ReactNode;
	tag?: ElementType;
	headerTag?: ElementType;
	overWriteColor?: null | TColor;
	parentId?: string | number | null;
	activeItem?: TActiveItemId;
	setActiveItem?: any;
}
export const AccordionItem = forwardRef<HTMLDivElement, IAccordionItemProps>(
	(
		{ id, icon, title, children, tag = 'div', headerTag = 'h2', overWriteColor, ...props },
		ref,
	) => {
		const ACTIVE = props.activeItem === id;

		return (
			<TagWrapper tag={tag} ref={ref} className={classNames('accordion-item')}>
				<TagWrapper tag={headerTag} className={classNames('accordion-header')} id={id}>
					<button
						className={classNames('accordion-button', {
							collapsed: !ACTIVE,
							[`accordion-button-${overWriteColor}`]: overWriteColor,
						})}
						type='button'
						data-bs-toggle='collapse'
						data-bs-target={`#${id}Collapse`}
						aria-expanded={ACTIVE}
						aria-controls={`${id}Collapse`}
						onClick={() =>
							ACTIVE ? props.setActiveItem(null) : props.setActiveItem(id)
						}>
						{icon && <Icon icon={icon} className='accordion-icon' />}
						{title}
					</button>
				</TagWrapper>
				<Collapse
					isOpen={ACTIVE}
					id={`${id}Collapse`}
					className={classNames('accordion-collapse')}
					aria-labelledby={id}
					data-bs-parent={`#${props.parentId}`}>
					<div className={classNames('accordion-body')}>{children}</div>
				</Collapse>
			</TagWrapper>
		);
	},
);
AccordionItem.displayName = 'AccordionItem';

interface IAccordionProps {
	tag?: 'div' | 'section';
	id: string | number;
	activeItemId?: TActiveItemId;
	children: ReactElement<IAccordionItemProps> | ReactElement<IAccordionItemProps>[] | Array<any>;
	shadow?: null | 'none' | 'sm' | 'default' | 'lg';
	color?: TColor;
	isFlush?: boolean;
	className?: string;
}
const Accordion = forwardRef<HTMLDivElement | HTMLTableSectionElement, IAccordionProps>(
	(
		{
			tag = 'div',
			id,
			activeItemId,
			children,
			shadow = 'default',
			color = 'primary',
			isFlush,
			className,
		},
		ref,
	) => {
		const [activeItem, setActiveItem] = useState<TActiveItemId>(
			activeItemId === false
				? null
				: activeItemId || (Array.isArray(children) && children?.flat()[0].props.id),
		);

		return (
			<TagWrapper
				tag={tag}
				ref={ref}
				className={classNames(
					'accordion',
					{
						'accordion-flush': isFlush,
						'shadow-none': isFlush,
						[`shadow${shadow !== 'default' ? `-${shadow}` : ''}`]: !!shadow,
					},
					className,
				)}
				id={id}>
				{Children.map(children, (child) =>
					// @ts-ignore
					['AccordionItem'].includes(child?.type?.displayName) ? (
						cloneElement(child as unknown as ReactElement<any>, {
							activeItem,
							setActiveItem,
							parentId: id,
							overWriteColor: child?.props?.overWriteColor || color,
						})
					) : (
						<code className='d-block'>
							Only AccordionItem component should be used as a child.
						</code>
					),
				)}
			</TagWrapper>
		);
	},
);
Accordion.displayName = 'Accordion';

export default Accordion;
