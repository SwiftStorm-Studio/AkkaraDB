// @ts-check
import {defineConfig} from 'astro/config';
import starlight from '@astrojs/starlight';
import markdoc from '@astrojs/markdoc';

// https://astro.build/config
export default defineConfig({
	site: 'https://docs.swiftstorm.dev',
	base: '/akkaradb-native',
	outDir: './dist/akkaradb-native',
	integrations: [
		markdoc(),
		starlight({
			title: 'AKkaraDB Native',
			customCss: ['./src/styles/oled.css'],
			components: {
				ThemeProvider: './src/components/ThemeProvider.astro',
				ThemeSelect: './src/components/ThemeSelect.astro',
			},
			defaultLocale: 'root',
			locales: {
				root: { label: 'English', lang: 'en' },
				ja: { label: '日本語', lang: 'ja' },
			},
			social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/SwiftStorm-Studio/AkkaraDB' }],
			sidebar: [
				{
					label: 'Guides',
					translations: { ja: 'ガイド' },
					items: [
						{ label: 'Overview', translations: { ja: '概要' }, slug: '' },
						{ label: 'Low-Level API', translations: { ja: '低レベル API' }, slug: 'api/low-level' },
					],
				},
			],
		}),
	],
});
