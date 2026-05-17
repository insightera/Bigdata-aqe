import React from 'react';
import dynamic from 'next/dynamic';
import { demoPagesMenu } from '../menu';

const DefaultAside = dynamic(() => import('../pages/_layout/_asides/DefaultAside'));

const asides = [
	{ path: demoPagesMenu.login.path, element: null, exact: true },
	{ path: demoPagesMenu.signUp.path, element: null, exact: true },
	{ path: '/*', element: <DefaultAside />, exact: true },
];

export default asides;
