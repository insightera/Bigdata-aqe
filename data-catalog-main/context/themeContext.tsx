import React, { createContext, FC, ReactNode, useEffect, useMemo, useState } from 'react';
import useDeviceScreen from '../hooks/useDeviceScreen';

export interface IThemeContextProps {
	asideStatus: boolean;
	darkModeStatus: boolean;
	fullScreenStatus: boolean;
	leftMenuStatus: boolean;
	mobileDesign: boolean;
	rightMenuStatus: boolean;
	rightPanel: boolean;
	setAsideStatus: (value: ((prevState: boolean) => boolean) | boolean) => void;
	setDarkModeStatus: (value: ((prevState: boolean) => boolean) | boolean) => void;
	setFullScreenStatus: (value: ((prevState: boolean) => boolean) | boolean) => void;
	setLeftMenuStatus: (value: ((prevState: boolean) => boolean) | boolean) => void;
	setRightMenuStatus: (value: ((prevState: boolean) => boolean) | boolean) => void;
	setRightPanel: (value: ((prevState: boolean) => boolean) | boolean) => void;
}
const ThemeContext = createContext<IThemeContextProps>({} as IThemeContextProps);

interface IThemeContextProviderProps {
	children: ReactNode;
}
/** Selaras dengan Bootstrap `md` (layar &lt; 768px = mobile). */
const MOBILE_MAX_WIDTH = Number(process.env.NEXT_PUBLIC_MOBILE_BREAKPOINT_SIZE ?? '767');
const ASIDE_MINIMIZE_FROM = Number(process.env.NEXT_PUBLIC_ASIDE_MINIMIZE_BREAKPOINT_SIZE ?? '992');

export const ThemeContextProvider: FC<IThemeContextProviderProps> = ({ children }) => {
	const deviceScreen = useDeviceScreen();
	const w = deviceScreen?.width ?? 0;
	const mobileDesign = w <= MOBILE_MAX_WIDTH;

	const [darkModeStatus, setDarkModeStatus] = useState(
		typeof window !== 'undefined' && localStorage.getItem('facit_darkModeStatus')
			? localStorage.getItem('facit_darkModeStatus') === 'true'
			: process.env.NEXT_PUBLIC_DARK_MODE === 'true',
	);

	useEffect(() => {
		localStorage.setItem('facit_darkModeStatus', darkModeStatus.toString());
	}, [darkModeStatus]);

	const [fullScreenStatus, setFullScreenStatus] = useState(false);

	const [leftMenuStatus, setLeftMenuStatus] = useState(false);
	const [rightMenuStatus, setRightMenuStatus] = useState(false);

	const [asideStatus, setAsideStatus] = useState(() => {
		if (typeof window === 'undefined') return false;
		const stored = localStorage.getItem('facit_asideStatus');
		if (stored !== null) return stored === 'true';
		return window.innerWidth >= ASIDE_MINIMIZE_FROM;
	});
	useEffect(() => {
		localStorage.setItem('facit_asideStatus', asideStatus?.toString());
	}, [asideStatus]);

	const [rightPanel, setRightPanel] = useState(false);

	useEffect(() => {
		const width = deviceScreen?.width ?? 0;
		if (width >= ASIDE_MINIMIZE_FROM) {
			// eslint-disable-next-line react-hooks/set-state-in-effect
			if (localStorage.getItem('facit_asideStatus') === 'true') setAsideStatus(true);
			setLeftMenuStatus(false);
			setRightMenuStatus(false);
		}
	}, [deviceScreen?.width]);

	const values: IThemeContextProps = useMemo(
		() => ({
			mobileDesign,
			darkModeStatus,
			setDarkModeStatus,
			fullScreenStatus,
			setFullScreenStatus,
			asideStatus,
			setAsideStatus,
			leftMenuStatus,
			setLeftMenuStatus,
			rightMenuStatus,
			setRightMenuStatus,
			rightPanel,
			setRightPanel,
		}),
		[
			asideStatus,
			darkModeStatus,
			fullScreenStatus,
			leftMenuStatus,
			mobileDesign,
			rightMenuStatus,
			rightPanel,
		],
	);

	return <ThemeContext.Provider value={values}>{children}</ThemeContext.Provider>;
};

export default ThemeContext;
