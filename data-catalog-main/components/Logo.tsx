import React, { FC } from 'react';

interface ILogoProps {
	width?: number;
	height?: number;
}
const Logo: FC<ILogoProps> = ({ width, height = 32 }) => {
	const w = width || height * 5;
	return (
		<svg
			width={w}
			height={height}
			viewBox='0 0 200 40'
			fill='none'
			xmlns='http://www.w3.org/2000/svg'>
			{/* Database icon */}
			<ellipse cx='20' cy='12' rx='12' ry='5' fill='currentColor' />
			<path
				d='M8 12v8c0 2.76 5.37 5 12 5s12-2.24 12-5v-8'
				stroke='currentColor'
				strokeWidth='2'
				fill='none'
			/>
			<path
				d='M8 20v8c0 2.76 5.37 5 12 5s12-2.24 12-5v-8'
				stroke='currentColor'
				strokeWidth='2'
				fill='none'
			/>
			{/* Text */}
			<text x='40' y='22' fontFamily='system-ui, sans-serif' fontSize='14' fontWeight='700' fill='currentColor'>
				Data Catalog
			</text>
			<text x='40' y='34' fontFamily='system-ui, sans-serif' fontSize='8' fontWeight='400' fill='currentColor' opacity='0.6'>
				Metadata Lakehouse
			</text>
		</svg>
	);
};

export default Logo;
