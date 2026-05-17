import React from 'react';
import Link from 'next/link';
import Footer from '../../../layout/Footer/Footer';
import Logo from '../../../components/Logo';

const DefaultFooter = () => {
	const year = new Date().getFullYear();

	return (
		<Footer>
			<div className='container-fluid footer-catalog footer-catalog--compact'>
				<div className='footer-catalog-row d-flex flex-wrap align-items-center justify-content-between gap-2 py-2'>
					<Link
						href='/'
						className='footer-catalog-brand text-decoration-none text-primary d-inline-flex align-items-center'>
						<Logo height={24} />
					</Link>
					<span className='footer-catalog-copy small text-muted text-md-end'>
						&copy; {year} Data Lakehouse Metadata
					</span>
				</div>
			</div>
		</Footer>
	);
};

export default DefaultFooter;
