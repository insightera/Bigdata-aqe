import React from 'react';
import { demoPagesMenu } from '../menu';
import DefaultFooter from '../pages/_layout/_footers/DefaultFooter';

const footers = [
	{ path: demoPagesMenu.login.path, element: null, exact: true },
	{ path: demoPagesMenu.signUp.path, element: null, exact: true },
	{ path: demoPagesMenu.page404.path, element: null, exact: true },
	{ path: '/*', element: <DefaultFooter />, exact: true },
];

export default footers;
