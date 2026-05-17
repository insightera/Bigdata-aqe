export const catalogMenu = {
	catalogOverview: {
		id: 'catalogOverview',
		text: 'Catalog',
		icon: 'Category',
	},
	dashboard: {
		id: 'dashboard',
		text: 'Dashboard',
		path: '/',
		icon: 'Dashboard',
		subMenu: null,
	},
	browse: {
		id: 'browse',
		text: 'Browse Datasets',
		path: 'catalog',
		icon: 'Storage',
		subMenu: null,
	},
	lineage: {
		id: 'lineage',
		text: 'Data Lineage',
		path: 'lineage',
		icon: 'AccountTree',
		subMenu: null,
	},
	kpiDashboard: {
		id: 'kpiDashboard',
		text: 'KPI Dashboard',
		path: 'kpi',
		icon: 'BarChart',
		subMenu: null,
	},
};

export const governanceMenu = {
	governanceOverview: {
		id: 'governanceOverview',
		text: 'Governance',
		icon: 'Security',
	},
	classifications: {
		id: 'classifications',
		text: 'Classifications',
		path: 'classifications',
		icon: 'Label',
		subMenu: null,
	},
	glossary: {
		id: 'glossary',
		text: 'Glossary',
		path: 'glossary',
		icon: 'MenuBook',
		subMenu: null,
	},
	quality: {
		id: 'quality',
		text: 'Data Quality',
		path: 'quality',
		icon: 'VerifiedUser',
		subMenu: null,
	},
};

export const pipelineMenu = {
	pipelineOverview: {
		id: 'pipelineOverview',
		text: 'Pipeline',
		icon: 'Settings',
	},
	layers: {
		id: 'layers',
		text: 'Medallion Layers',
		path: 'layers',
		icon: 'Layers',
		subMenu: {
			bronze: {
				id: 'bronze',
				text: 'Bronze (Raw)',
				path: 'layers/bronze',
				icon: 'RawOn',
			},
			silver: {
				id: 'silver',
				text: 'Silver (Enriched)',
				path: 'layers/silver',
				icon: 'AutoFixHigh',
			},
			gold: {
				id: 'gold',
				text: 'Gold (Star Schema)',
				path: 'layers/gold',
				icon: 'Star',
			},
		},
	},
	umt: {
		id: 'umt',
		text: 'Unified Metadata (UMT)',
		path: 'umt',
		icon: 'TableChart',
		subMenu: null,
	},
	metadataQuality: {
		id: 'metadataQuality',
		text: 'Metadata Quality (§4.1.6)',
		path: 'metadata-quality',
		icon: 'Assessment',
		subMenu: null,
	},
};

export const pageLayoutTypesPagesMenu = {
	layoutTypes: {
		id: 'layoutTypes',
		text: 'Page Layout Types',
	},
	blank: {
		id: 'blank',
		text: 'Blank',
		path: 'page-layouts/blank',
		icon: 'check_box_outline_blank ',
	},
	pageLayout: {
		id: 'pageLayout',
		text: 'Page Layout',
		path: 'page-layouts',
		icon: 'BackupTable',
		subMenu: {
			headerAndSubheader: {
				id: 'headerAndSubheader',
				text: 'Header & Subheader',
				path: 'page-layouts/header-and-subheader',
				icon: 'ViewAgenda',
			},
			onlyHeader: {
				id: 'onlyHeader',
				text: 'Only Header',
				path: 'page-layouts/only-header',
				icon: 'ViewStream',
			},
			onlySubheader: {
				id: 'onlySubheader',
				text: 'Only Subheader',
				path: 'page-layouts/only-subheader',
				icon: 'ViewStream',
			},
			onlyContent: {
				id: 'onlyContent',
				text: 'Only Content',
				path: 'page-layouts/only-content',
				icon: 'WebAsset',
			},
		},
	},
};

export const demoPagesMenu = {
	auth: {
		id: 'auth',
		text: 'Auth Pages',
		icon: 'Extension',
	},
	login: {
		id: 'login',
		text: 'Login',
		path: 'auth-pages/login',
		icon: 'Login',
	},
	signUp: {
		id: 'signUp',
		text: 'Sign Up',
		path: 'auth-pages/sign-up',
		icon: 'PersonAdd',
	},
	page404: {
		id: 'Page404',
		text: '404 Page',
		path: '404',
		icon: 'ReportGmailerrorred',
	},
};
