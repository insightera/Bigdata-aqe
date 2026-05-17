import React, { useRef, useState, useCallback, JSX } from 'react';
import classNames from 'classnames';
import { Transition } from 'react-transition-group';
import { TransitionStatuses, TransitionTimeouts } from './utils';

type TCarouselItemProps = {
	tag?: keyof JSX.IntrinsicElements;
	in?: boolean;
	children?: React.ReactNode;
	slide?: boolean;
	className?: string;
	isFluid?: boolean;
	direction?: 'start' | 'end';
	onEnter?: (node: HTMLElement, isAppearing: boolean) => void;
	onEntering?: (node: HTMLElement, isAppearing: boolean) => void;
	onExit?: (node: HTMLElement) => void;
	onExiting?: (node: HTMLElement) => void;
	onExited?: (node: HTMLElement) => void;
};

const CarouselItem: React.FC<TCarouselItemProps> = ({
	in: inProp,
	children,
	slide = true,
	className,
	isFluid = false,
	direction = 'end',
	onEnter,
	onEntering,
	onExit,
	onExiting,
	onExited,
}) => {
	const nodeRef = useRef<HTMLDivElement>(null);
	const [startAnimation, setStartAnimation] = useState(false);

	const handleEnter = useCallback(() => {
		setStartAnimation(false);
		if (nodeRef.current) onEnter?.(nodeRef.current, false);
	}, [onEnter]);

	const handleEntering = useCallback(() => {
		if (nodeRef.current) {
			void nodeRef.current.offsetHeight; // trigger reflow
			setStartAnimation(true);
			onEntering?.(nodeRef.current, false);
		}
	}, [onEntering]);

	const handleExit = useCallback(() => {
		setStartAnimation(false);
		if (nodeRef.current) onExit?.(nodeRef.current);
	}, [onExit]);

	const handleExiting = useCallback(() => {
		if (nodeRef.current) {
			setStartAnimation(true);
			nodeRef.current.dispatchEvent(new CustomEvent('slide.bs.carousel'));
			onExiting?.(nodeRef.current);
		}
	}, [onExiting]);

	const handleExited = useCallback(() => {
		if (nodeRef.current) {
			nodeRef.current.dispatchEvent(new CustomEvent('slid.bs.carousel'));
			onExited?.(nodeRef.current);
		}
	}, [onExited]);

	return (
		<Transition
			in={inProp}
			timeout={TransitionTimeouts.Carousel}
			nodeRef={nodeRef}
			enter={slide}
			exit={slide}
			onEnter={handleEnter}
			onEntering={handleEntering}
			onExit={handleExit}
			onExiting={handleExiting}
			onExited={handleExited}>
			{(status) => {
				const isActive =
					status === TransitionStatuses.ENTERED || status === TransitionStatuses.EXITING;

				const directionClassName =
					(status === TransitionStatuses.ENTERING ||
						status === TransitionStatuses.EXITING) &&
					startAnimation &&
					(direction === 'end' ? 'carousel-item-start' : 'carousel-item-end');

				const orderClassName =
					status === TransitionStatuses.ENTERING &&
					(direction === 'end' ? 'carousel-item-next' : 'carousel-item-prev');

				const itemClasses = classNames(
					className,
					'carousel-item',
					isActive && 'active',
					directionClassName,
					orderClassName,
					{ 'h-100': isFluid },
				);

				return (
					<div ref={nodeRef} className={itemClasses}>
						{children}
					</div>
				);
			}}
		</Transition>
	);
};

export default CarouselItem;
