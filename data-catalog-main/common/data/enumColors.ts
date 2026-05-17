import { TColor } from '../../type/color-type';

export interface IColors {
	[key: string]: {
		name: TColor;
		code: string;
	};
}
const COLORS: IColors = {
	PRIMARY: {
		name: 'primary',
		code: String(process.env.NEXT_PUBLIC_PRIMARY_COLOR),
	},
	SECONDARY: {
		name: 'secondary',
		code: String(process.env.NEXT_PUBLIC_SECONDARY_COLOR),
	},
	SUCCESS: {
		name: 'success',
		code: String(process.env.NEXT_PUBLIC_SUCCESS_COLOR),
	},
	INFO: {
		name: 'info',
		code: String(process.env.NEXT_PUBLIC_INFO_COLOR),
	},
	WARNING: {
		name: 'warning',
		code: String(process.env.NEXT_PUBLIC_WARNING_COLOR),
	},
	DANGER: {
		name: 'danger',
		code: String(process.env.NEXT_PUBLIC_DANGER_COLOR),
	},
	DARK: {
		name: 'dark',
		code: String(process.env.NEXT_PUBLIC_DARK_COLOR),
	},
	LIGHT: {
		name: 'light',
		code: String(process.env.NEXT_PUBLIC_LIGHT_COLOR),
	},
};

export function getColorNameWithIndex(_index: number) {
	return COLORS.PRIMARY.name;
}

export default COLORS;
