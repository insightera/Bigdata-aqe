import React, { FC, HTMLAttributes, ReactNode } from 'react';
import classNames from 'classnames';
import Avatar from './Avatar';
import useDarkMode from '../hooks/useDarkMode';
import { TColor } from '../type/color-type';

interface IChatAvatarProps extends HTMLAttributes<HTMLDivElement> {
	src?: string;
	className?: string;
	color?: TColor | 'link' | 'brand' | 'brand-two' | 'storybook';
	unreadMessage?: number;
	isOnline?: boolean;
	size?: number;
}
export const ChatAvatar: FC<IChatAvatarProps> = ({
	src,
	className,
	color,
	unreadMessage,
	isOnline,
	size = 45,
	...props
}) => {
	return (
		<div className={classNames('chat-avatar', className)} {...props}>
			<div className='position-relative'>
				{src && <Avatar src={src} size={size} color={color} />}
				{unreadMessage && (
					<span className='position-absolute top-15 start-85 translate-middle badge rounded-pill bg-danger'>
						{unreadMessage} <span className='visually-hidden'>unread messages</span>
					</span>
				)}
				{isOnline && (
					<span className='position-absolute top-85 start-85 translate-middle badge border border-2 border-light rounded-circle bg-success p-2'>
						<span className='visually-hidden'>Online user</span>
					</span>
				)}
			</div>
		</div>
	);
};

interface IChatListItemProps extends HTMLAttributes<HTMLDivElement> {
	src: string;
	className?: string;
	isOnline?: boolean;
	color?: TColor | 'link' | 'brand' | 'brand-two' | 'storybook';
	size?: number;
	name: string;
	surname: string;
	latestMessage?: string;
	unreadMessage?: number;
	isActive?: boolean;
	lastSeenTime?: string;
}
export const ChatListItem: FC<IChatListItemProps> = ({
	src,
	className,
	isOnline,
	color = 'primary',
	size = 64,
	name,
	surname,
	latestMessage,
	unreadMessage,
	isActive,
	lastSeenTime,
	...props
}) => {
	const { darkModeStatus } = useDarkMode();

	return (
		<div className={classNames('col-12 cursor-pointer', className)} {...props}>
			<div
				className={classNames(
					'd-flex align-items-center',
					'p-3 rounded-2',
					'transition-base',
					{
						'bg-l25-info-hover': !darkModeStatus,
						'bg-lo50-info-hover': darkModeStatus,
						'bg-l10-info': !darkModeStatus && isActive,
						'bg-lo25-info': darkModeStatus && isActive,
					},
				)}>
				<ChatAvatar
					src={src}
					isOnline={isOnline}
					unreadMessage={unreadMessage}
					color={color}
					size={size}
					className='me-3'
				/>
				<div className='d-grid'>
					<div className='d-flex flex-wrap d-xxl-block'>
						<span className='fw-bold fs-5 me-3'>{`${name} ${surname}`}</span>
						{lastSeenTime && (
							<small
								className={classNames(
									'text-info fw-bold px-3 py-1 rounded-pill align-top text-nowrap',
									{
										'bg-l10-info': !darkModeStatus,
										'bg-lo25-info': darkModeStatus,
									},
								)}>
								{lastSeenTime}
							</small>
						)}
					</div>
					<div className='text-muted text-truncate'>{latestMessage}</div>
				</div>
			</div>
		</div>
	);
};

interface IChatHeaderProps {
	to: string;
}
export const ChatHeader: FC<IChatHeaderProps> = ({ to }) => {
	return (
		<>
			<strong className='me-2'>To:</strong>
			{to}
		</>
	);
};

interface IChatMessagesProps extends HTMLAttributes<HTMLDivElement> {
	messages: {
		id?: string | number;
		message?: string | number;
	}[];
	isReply?: boolean;
}
export const ChatMessages: FC<IChatMessagesProps> = ({ messages, isReply, ...props }) => {
	return (
		<div className='chat-messages' {...props}>
			{messages.map((i) => (
				<div
					key={i.id}
					className={classNames('chat-message', { 'chat-message-reply': isReply })}>
					{i.message}
				</div>
			))}
		</div>
	);
};

interface IChatGroupProps extends HTMLAttributes<HTMLDivElement> {
	isReply?: boolean;
	messages: {
		id?: string | number;
		message?: string | number;
	}[];
	isOnline?: boolean;
	color?: TColor | 'link' | 'brand' | 'brand-two' | 'storybook';
	user: {
		src?: string;
		username?: string;
		name?: string;
		surname?: string;
		isOnline?: boolean;
		color?: TColor | 'link' | 'brand' | 'brand-two' | 'storybook';
	};
}
export const ChatGroup: FC<IChatGroupProps> = ({
	isReply,
	messages,
	// eslint-disable-next-line @typescript-eslint/no-unused-vars
	isOnline,
	// eslint-disable-next-line @typescript-eslint/no-unused-vars
	color,
	user,
	...props
}) => {
	const AVATAR = <ChatAvatar src={user.src} isOnline={user.isOnline} color={user.color} />;
	return (
		<div className={classNames('chat-group', { 'chat-group-reply': isReply })} {...props}>
			{!isReply && AVATAR}
			<ChatMessages messages={messages} isReply={isReply} />
			{isReply && AVATAR}
		</div>
	);
};

interface IChatProps {
	children: ReactNode;
	className?: string;
}
const Chat: FC<IChatProps> = ({ children, className }) => {
	return <div className={classNames('chat-container', className)}>{children}</div>;
};

export default Chat;
