import React from 'react';
import Header, { HeaderLeft, HeaderRight } from '../../../layout/Header/Header';
import Navigation from '../../../layout/Navigation/Navigation';
import { catalogMenu } from '../../../menu';
import useDeviceScreen from '../../../hooks/useDeviceScreen';
import Icon from '../../../components/icon/Icon';

const DefaultHeader = () => {
	const deviceScreen = useDeviceScreen();
	return (
		<Header>
			<HeaderLeft>
				<Navigation
					menu={{ ...catalogMenu }}
					id='header-top-menu'
					horizontal={
						!!deviceScreen?.width &&
						deviceScreen.width >= Number(process.env.NEXT_PUBLIC_MOBILE_BREAKPOINT_SIZE)
					}
				/>
			</HeaderLeft>
			<HeaderRight>
				<div className='d-flex align-items-center'>
					<Icon icon='Storage' color='primary' className='me-2' />
					<span className='fw-bold'>Data Catalog Portal</span>
				</div>
			</HeaderRight>
		</Header>
	);
};

export default DefaultHeader;
