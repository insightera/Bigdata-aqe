import React from 'react';
import { catalogMenu, demoPagesMenu } from '../menu';
import DashboardHeader from '../pages/_layout/_headers/DashboardHeader';

const headers = [
	{ path: demoPagesMenu.login.path, element: null },
	{ path: demoPagesMenu.signUp.path, element: null },
	{ path: demoPagesMenu.page404.path, element: null },
	{ path: catalogMenu.dashboard.path, element: <DashboardHeader /> },
	{
		path: `/*`,
		element: <DashboardHeader />,
	},
];

export default headers;
