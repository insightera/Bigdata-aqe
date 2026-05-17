const { i18n } = require('./next-i18next.config');
const sass = require('sass');
const withImages = require('next-images');
const withInterceptStdout = require('next-intercept-stdout');

var hideWarn = [
	'Unrecognized key(s) in object: \'serverRuntimeConfig\'',
	'Invalid next.config.js options detected:',
	'The value at .experimental has an unexpected property, images, which is not in the list of allowed properties',
	'https://nextjs.org/docs/messages/invalid-next-config',
	'You have enabled experimental feature (images) in next.config.js.',
	'Experimental features are not covered by semver, and may cause unexpected or broken application behavior. Use at your own risk.',
	'Fast Refresh had to perform a full reload.',
	"Cannot read properties of null (reading 'length')"
];

const nextConfig = withInterceptStdout(
	withImages({
		images: {
			disableStaticImages: true
		},
		reactStrictMode: true,
		// Host yang boleh memuat aset dev /_next/* (HMR, dll.) dari luar localhost — tanpa ini Next 16 memblokir.
		allowedDevOrigins: [
			'catalog.insightera.cloud',
			'www.catalog.insightera.cloud',
			'103.174.114.177',
			'localhost',
			'127.0.0.1',
			...(process.env.NEXT_ALLOWED_DEV_ORIGINS || '')
				.split(',')
				.map((h) => h.trim())
				.filter(Boolean),
		],
		i18n,
		// webpack(config, options) {
		// 	return config
		// },
		turbopack: {},
		sassOptions: {
			quietDeps: true,
			silenceDeprecations: ['import', 'global-builtin'],
			logger: sass.Logger.silent,
		},
	env: {
		NEXT_PUBLIC_ATLAS_URL: process.env.NEXT_PUBLIC_ATLAS_URL || 'http://localhost:21000',
		NEXT_PUBLIC_MOBILE_BREAKPOINT_SIZE: process.env.NEXT_PUBLIC_MOBILE_BREAKPOINT_SIZE || '767',
		NEXT_PUBLIC_ASIDE_MINIMIZE_BREAKPOINT_SIZE:
			process.env.NEXT_PUBLIC_ASIDE_MINIMIZE_BREAKPOINT_SIZE || '992',
	},
	webpack(config, options) {
		return config;
	},
	}),
	(log) => (hideWarn.some((warn) => log.includes(warn)) ? '' : log),
);

module.exports = nextConfig;
