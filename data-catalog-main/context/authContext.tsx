import React, { createContext, FC, ReactNode, useEffect, useMemo, useState } from 'react';
import { getUserDataWithUsername, IUserProps } from '../common/data/userDummyData';

export interface IAuthContextProps {
	user: string;
	setUser?(...args: unknown[]): unknown;
	userData: Partial<IUserProps>;
}
const AuthContext = createContext<IAuthContextProps>({} as IAuthContextProps);

interface IAuthContextProviderProps {
	children: ReactNode;
}
export const AuthContextProvider: FC<IAuthContextProviderProps> = ({ children }) => {
	// @ts-ignore
	const [user, setUser] = useState<string>(
		typeof window !== 'undefined' ? String(localStorage?.getItem('facit_authUsername')) : '',
	);
	const [userData, setUserData] = useState<Partial<IUserProps>>({});

	useEffect(() => {
		localStorage.setItem('facit_authUsername', user);
	}, [user]);

	useEffect(() => {
		if (user !== '') {
			// eslint-disable-next-line react-hooks/set-state-in-effect
			setUserData(getUserDataWithUsername(user));
		} else {
			setUserData({});
		}
	}, [user]);

	const value = useMemo(
		() => ({
			user,
			setUser,
			userData,
		}),
		[user, userData],
	);
	return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
};

export default AuthContext;
