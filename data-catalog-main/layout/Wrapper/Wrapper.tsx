import React, { FC, ReactNode, useContext } from 'react';
import classNames from 'classnames';
import Content from '../Content/Content';
import WrapperOverlay from './WrapperOverlay';
import HeaderRoutes from '../Header/HeaderRoutes';
import FooterRoutes from '../Footer/FooterRoutes';
import ThemeContext from '../../context/themeContext';

interface IWrapperContainerProps {
	children: ReactNode;
	className?: string;
}
export const WrapperContainer: FC<IWrapperContainerProps> = ({ children, className, ...props }) => {
	const { rightPanel } = useContext(ThemeContext);
	return (
		<div
			className={classNames(
				'wrapper',
				{ 'wrapper-right-panel-active': rightPanel },
				className,
			)}
			{...props}>
			{children}
		</div>
	);
};

interface IWrapper {
	children: ReactNode;
}
const Wrapper: FC<IWrapper> = ({ children }) => {
	return (
		<>
			<WrapperContainer>
				<HeaderRoutes />
				<Content>{children}</Content>
				<FooterRoutes />
			</WrapperContainer>
			<WrapperOverlay />
		</>
	);
};

export default Wrapper;
