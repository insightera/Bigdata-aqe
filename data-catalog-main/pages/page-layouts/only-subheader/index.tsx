import React from 'react';
import type { NextPage } from 'next';
import { GetStaticProps } from 'next';
import { serverSideTranslations } from 'next-i18next/serverSideTranslations';
import Head from 'next/head';
import PageWrapper from '../../../layout/PageWrapper/PageWrapper';
import { pageLayoutTypesPagesMenu } from '../../../menu';
import SubHeader, { SubHeaderLeft, SubHeaderRight } from '../../../layout/SubHeader/SubHeader';
import Breadcrumb from '../../../components/bootstrap/Breadcrumb';
import Page from '../../../layout/Page/Page';
import Popovers from '../../../components/bootstrap/Popovers';

const Index: NextPage = () => {
	return (
		<PageWrapper>
			<Head>
				<title>{pageLayoutTypesPagesMenu.pageLayout.subMenu.onlySubheader.text}</title>
			</Head>
			<SubHeader>
				<SubHeaderLeft>
					<Breadcrumb
						list={[
							{ title: 'Page Layout', to: '/page-layouts' },
							{
								title: 'Only Subheader',
								to: '/page-layouts/only-subheader',
							},
						]}
					/>
				</SubHeaderLeft>
				<SubHeaderRight>
					<Popovers
						title='index.tsx'
						desc={<code>pages/page-layouts/only-subheader/index.tsx</code>}>
						SubHeaderRight
					</Popovers>
					<code>index.tsx</code>
				</SubHeaderRight>
			</SubHeader>
			<Page>
				<div className='row d-flex align-items-center h-100'>
					<div
						className='col-12 d-flex justify-content-center align-items-center'
						style={{ fontSize: 'calc(1rem + 1vw)' }}>
						<Popovers
							title='index.tsx'
							desc={<code>pages/page-layouts/only-subheader/index.tsx</code>}>
							Page
						</Popovers>
						<code className='ps-3'>index.tsx</code>
					</div>
				</div>
			</Page>
		</PageWrapper>
	);
};

export const getStaticProps: GetStaticProps = async ({ locale }) => ({
	props: {
		// @ts-ignore
		...(await serverSideTranslations(locale, ['common', 'menu'])),
	},
});

export default Index;
