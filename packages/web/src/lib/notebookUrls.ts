import type { Theme } from '@/context/ThemeContext';

import { notebookQueryParams } from '@marimo-hub/notebook-bridge/query';
export { notebookQueryParams } from '@marimo-hub/notebook-bridge/query';

export function notebookFrameUrl(
	url: string,
	search: string,
	theme: Theme,
	isApp: boolean,
	present = false,
): string {
	try {
		const parsed = new URL(url, window.location.origin);
		const trustedKeys = new Set(parsed.searchParams.keys());
		for (const [key, value] of notebookQueryParams(search)) {
			if (!trustedKeys.has(key)) parsed.searchParams.append(key, value);
		}
		parsed.searchParams.set('theme', theme);
		if (isApp) parsed.searchParams.set('show-code', 'false');
		else if (present) parsed.searchParams.set('view-as', 'present');
		return parsed.toString();
	} catch {
		return url;
	}
}
