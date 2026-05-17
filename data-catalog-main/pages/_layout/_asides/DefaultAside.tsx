import React, { useContext } from 'react';
import Brand from '../../../layout/Brand/Brand';
import Navigation, { NavigationLine } from '../../../layout/Navigation/Navigation';
import { catalogMenu, governanceMenu, pipelineMenu } from '../../../menu';
import ThemeContext from '../../../context/themeContext';
import Aside, { AsideBody, AsideHead } from '../../../layout/Aside/Aside';

const DefaultAside = () => {
	const { asideStatus, setAsideStatus } = useContext(ThemeContext);

	return (
		<Aside>
			<AsideHead>
				<Brand asideStatus={asideStatus} setAsideStatus={setAsideStatus} />
			</AsideHead>
			<AsideBody>
				<Navigation menu={catalogMenu} id='aside-catalog' />
				<NavigationLine />
				<Navigation menu={governanceMenu} id='aside-governance' />
				<NavigationLine />
				<Navigation menu={pipelineMenu} id='aside-pipeline' />
			</AsideBody>
		</Aside>
	);
};

export default DefaultAside;
