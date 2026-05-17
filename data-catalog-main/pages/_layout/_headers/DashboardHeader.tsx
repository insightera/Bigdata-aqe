import React, { useState } from 'react';
import { useRouter } from 'next/router';
import Header, { HeaderLeft, HeaderRight } from '../../../layout/Header/Header';
import Button, { IButtonProps } from '../../../components/bootstrap/Button';
import Icon from '../../../components/icon/Icon';
import Input from '../../../components/bootstrap/forms/Input';
import InputGroup, { InputGroupText } from '../../../components/bootstrap/forms/InputGroup';
import useDarkMode from '../../../hooks/useDarkMode';

const DashboardHeader = () => {
	const router = useRouter();
	const { darkModeStatus, setDarkModeStatus } = useDarkMode();
	const [searchQuery, setSearchQuery] = useState('');

	const styledBtn: IButtonProps = {
		color: darkModeStatus ? 'dark' : 'light',
		hoverShadow: 'default',
		isLight: !darkModeStatus,
		size: 'lg',
	};

	const handleSearch = (e: React.FormEvent) => {
		e.preventDefault();
		if (searchQuery.trim()) {
			router.push(`/catalog?q=${encodeURIComponent(searchQuery.trim())}`);
		}
	};

	return (
		<Header>
			<HeaderLeft>
				<form onSubmit={handleSearch} className='d-flex align-items-center'>
					<InputGroup>
						<InputGroupText>
							<Icon icon='Search' />
						</InputGroupText>
						<Input
							placeholder='Search datasets, tables, columns...'
							value={searchQuery}
							onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
								setSearchQuery(e.target.value)
							}
							ariaLabel='Global search'
							style={{ minWidth: 300 }}
						/>
					</InputGroup>
					<Button color='primary' isLight type='submit' className='ms-2'>
						Search
					</Button>
				</form>
			</HeaderLeft>
			<HeaderRight>
				<div className='row g-3 align-items-center'>
					<div className='col-auto'>
						<Button
							{...styledBtn}
							icon='Dashboard'
							onClick={() => router.push('/')}>
							Dashboard
						</Button>
					</div>
					<div className='col-auto'>
						<Button
							{...styledBtn}
							onClick={() => setDarkModeStatus(!darkModeStatus)}
							className='btn-only-icon'>
							<Icon
								icon={darkModeStatus ? 'DarkMode' : 'LightMode'}
								color='primary'
								className='btn-icon'
							/>
						</Button>
					</div>
				</div>
			</HeaderRight>
		</Header>
	);
};

export default DashboardHeader;
