#!/usr/bin/env node

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const fs = require('fs');
const cheerio = require('cheerio');

puppeteer.use(StealthPlugin());

// --- Helper: Sleep ---
function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

// --- Configuration: User Agents (for logging only) ---
const USER_AGENTS = [
	'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/109.0',
	'Mozilla/5.0 (Macintosh; Intel Mac OS X 12_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.3 Safari/605.1.15',
	'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.5615.49 Safari/537.36',
	'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
	'Mozilla/5.0 (iPad; CPU OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1',
	'Mozilla/5.0 (Linux; Android 12; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.5563.64 Mobile Safari/537.36',
	'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_0_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
	'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:112.0) Gecko/20100101 Firefox/112.0',
	'Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.5615.49 Safari/537.36',
	'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1',
];

// --- Proxy List (Example – adjust your proxies here) ---
const PROXIES = [
	// Example: "https://user:password@ip:port",
];

// --- Files for Heartbeat and Progress ---
const HEARTBEAT_FILE = 'heartbeat_products.txt';
const PROGRESS_FILE = 'progress_products.txt'; // Progress file for product scraper

// --- Update heartbeat file ---
function updateHeartbeat(categoryIndex) {
	const heartbeatText = `${new Date().toISOString()} - Category Index: ${categoryIndex}\n`;
	console.log('Updating heartbeat:', heartbeatText.trim());
	fs.writeFileSync(HEARTBEAT_FILE, heartbeatText);
}

// --- Update progress file ---
function updateProgress(categoryIndex) {
	fs.writeFileSync(PROGRESS_FILE, String(categoryIndex));
}

// --- Get start index from progress file ---
function getStartIndex() {
	if (fs.existsSync(PROGRESS_FILE)) {
		try {
			const content = fs.readFileSync(PROGRESS_FILE, 'utf8').trim();
			const index = parseInt(content, 10);
			return index + 1; // resume from the next category
		} catch (err) {
			console.error('Error reading progress file:', err);
		}
	}
	return 0;
}

// --- Simulate human-like scrolling to trigger lazy loading ---
async function simulateHumanInteraction(page) {
	// Choose a random number of scroll events between 3 and 6
	const iterations = Math.floor(Math.random() * 4) + 3; // 3 to 6 times

	for (let i = 0; i < iterations; i++) {
		// Random wait time between 500ms and 2500ms before each scroll
		const waitTime = 500 + Math.random() * 2000;

		// Random scroll distance between 100 and 300 pixels
		const scrollDistance = 100 + Math.random() * 200;

		// Random direction: 80% chance to scroll down, 20% chance to scroll up
		const direction = Math.random() < 0.8 ? 1 : -1;

		await page.evaluate(
			(distance, direction) => {
				window.scrollBy(0, distance * direction);
			},
			scrollDistance,
			direction
		);

		await sleep(waitTime);
	}

	// Finally, scroll to the bottom of the page
	await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
	// Wait for a random duration between 4 and 8 seconds before proceeding
	await sleep(4000 + Math.random() * 4000);
}

// --- Create and configure the Puppeteer browser/page ---
async function getDriver() {
	const launchOptions = {
		headless: true,
		args: ['--disable-gpu', '--lang=en-US', '--window-size=1280,720', '--incognito', '--disable-blink-features=AutomationControlled', '--no-sandbox', '--disable-setuid-sandbox'],
	};

	// Set a random user agent
	const userAgent = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];

	// Rotate proxy if available
	if (PROXIES.length > 0) {
		const proxy = PROXIES[Math.floor(Math.random() * PROXIES.length)];
		console.log('Using proxy:', proxy);
		launchOptions.args.push(`--proxy-server=${proxy}`);
	}

	const browser = await puppeteer.launch(launchOptions);
	const page = await browser.newPage();
	await page.setUserAgent(userAgent);

	// Set extra HTTP headers with a random Accept-Language
	const acceptLanguages = ['en-US,en;q=0.9', 'de-DE,de;q=0.9', 'fr-FR,fr;q=0.9'];
	const selectedAcceptLanguage = acceptLanguages[Math.floor(Math.random() * acceptLanguages.length)];
	await page.setExtraHTTPHeaders({
		'Accept-Language': selectedAcceptLanguage,
	});

	// Wait for the browser to fully initialize
	await sleep(20000);
	return { browser, page };
}

// --- Load a URL with exponential backoff for rate-limiting/CAPTCHA ---
async function limitedGet(url, page, maxWait = 36000) {
	let attempt = 1;
	let totalWait = 0;
	while (true) {
		try {
			console.log(`Requesting: ${url} (attempt ${attempt})`);
			// Clear cookies to simulate a fresh session
			const cookies = await page.cookies();
			await page.deleteCookie(...cookies);

			// Capture the response from page.goto
			const response = await page.goto(url, { waitUntil: 'networkidle2' });
			if (response) {
				const status = response.status();
				// If a rate-limit header is detected (e.g., 403 or 429), log it
				if (status === 403 || status === 429) {
					console.log(`Detected rate limit with header status code: ${status}`);
				}
			}

			// Dismiss cookie popover if it exists
			try {
				const cookieButtonSelector = '#onetrust-accept-btn-handler'; // Use the id selector
				await page.waitForSelector(cookieButtonSelector, { timeout: 5000 });
				console.log('Cookie popover detected. Dismissing it.');
				await page.click(cookieButtonSelector);
				await sleep(2000); // Wait for the popup to disappear
			} catch (err) {
				// If the popup doesn't appear within 5 seconds, continue normally.
			}

			await simulateHumanInteraction(page);
			const delay = 10000 + Math.random() * 5000; // 10-15 sec delay
			await sleep(delay);
			const pageContent = await page.content();

			// Check if page content indicates rate limiting or CAPTCHA
			if (pageContent.toLowerCase().includes('too many requests')) {
				const delaySec = Math.min(120 * Math.pow(2, attempt - 1), maxWait);
				totalWait += delaySec;
				if (totalWait >= maxWait) {
					console.log(`Max wait time of ${maxWait} seconds reached. Aborting ${url}.`);
					break;
				}
				console.log(`Rate limit detected on ${url}. Waiting for ${delaySec} seconds before retrying...`);
				await sleep(delaySec * 1000);
				attempt++;
				continue;
			}

			if (pageContent.toLowerCase().includes('captcha') || pageContent.toLowerCase().includes('i am not a robot')) {
				const delaySec = Math.min(60 * Math.pow(2, attempt - 1), maxWait);
				totalWait += delaySec;
				if (totalWait >= maxWait) {
					console.log(`Max wait time of ${maxWait} seconds reached on CAPTCHA. Aborting ${url}.`);
					break;
				}
				console.log(`CAPTCHA detected on ${url}. Waiting for ${delaySec} seconds before retrying...`);
				await sleep(delaySec * 1000);
				attempt++;
				continue;
			}
			return pageContent;
		} catch (error) {
			const delaySec = Math.min(60 * Math.pow(2, attempt - 1), maxWait);
			totalWait += delaySec;
			if (totalWait >= maxWait) {
				console.log(`Max wait time of ${maxWait} seconds reached on exception. Aborting ${url}.`);
				break;
			}
			console.log(`Error loading ${url} on attempt ${attempt}: ${error}. Waiting for ${delaySec} seconds before next attempt...`);
			await sleep(delaySec * 1000);
			attempt++;
		}
	}
	console.log(`Failed to load ${url} after waiting a total of ${totalWait} seconds.`);
	return null;
}

// --- Extract the maximum number of pages from pagination ---
function getMaxPages(html) {
	const $ = cheerio.load(html);
	const pagination = $('ul.pagination');
	if (!pagination.length) return 1;
	let maxPage = 1;
	pagination.find('a[href]').each((i, elem) => {
		const href = $(elem).attr('href') || '';
		const match = href.match(/page=(\d+)/);
		if (match && match[1]) {
			const num = parseInt(match[1], 10);
			if (num > maxPage) {
				maxPage = num;
			}
		}
	});
	return maxPage;
}

// --- Get product links from a single page ---
async function getProductLinksFromPage(url, page) {
	const pageContent = await limitedGet(url, page);
	if (!pageContent) {
		console.log(`Failed to load page ${url}.`);
		return [];
	}
	if (pageContent.toLowerCase().includes('captcha') || pageContent.toLowerCase().includes('i am not a robot')) {
		console.log(`CAPTCHA detected on page ${url}. Skipping this page.`);
		return [];
	}
	const $ = cheerio.load(pageContent);
	const linksSet = new Set();
	$('a[href]').each((i, elem) => {
		const href = $(elem).attr('href') || '';
		if (href.startsWith('/reviews/') || href.includes('#reviews')) {
			let absoluteUrl = href;
			if (!href.startsWith('http')) {
				absoluteUrl = 'https://www.capterra.ch' + href;
			}
			linksSet.add(absoluteUrl);
		}
	});
	const delay = 10000 + Math.random() * 5000; // 10-15 sec delay
	console.log(`Sleeping for ${(delay / 1000).toFixed(2)} seconds to throttle requests.`);
	await sleep(delay);
	return Array.from(linksSet);
}

// --- Get all product links for a given category ---
async function getCategoryProductLinks(categoryLink, page) {
	console.log(`Fetching category page: ${categoryLink}`);
	const pageContent = await limitedGet(categoryLink, page);
	if (!pageContent) {
		throw new Error(`Error fetching category page: ${categoryLink}`);
	}
	const maxPages = getMaxPages(pageContent);
	console.log(`Found maximum pages: ${maxPages}`);
	const allProductLinks = new Set();
	for (let i = 1; i <= maxPages; i++) {
		const pageUrl = i === 1 ? categoryLink : `${categoryLink}?page=${i}`;
		console.log(`Processing page ${i}: ${pageUrl}`);
		const pageLinks = await getProductLinksFromPage(pageUrl, page);
		console.log(`Found ${pageLinks.length} product links on page ${i}`);
		pageLinks.forEach((link) => allProductLinks.add(link));
	}
	return Array.from(allProductLinks);
}

// --- Main function: Scrape all categories and their products ---
async function scrapeAllCategoriesProducts() {
	let categories;
	try {
		const categoriesData = fs.readFileSync('capterra_categories.json', 'utf8');
		categories = JSON.parse(categoriesData);
	} catch (error) {
		console.error('No categories found in capterra_categories.json or error reading file');
		return {};
	}
	if (!categories || categories.length === 0) {
		console.log('No categories found in capterra_categories.json');
		return {};
	}
	const results = {};
	const { browser, page } = await getDriver();
	const startIndex = getStartIndex();
	console.log(`Resuming from category index: ${startIndex}`);
	const csvFilePath = 'capterra_products.csv';
	if (startIndex === 0) {
		fs.writeFileSync(csvFilePath, 'Category,Product Link\n');
	}
	for (let idx = startIndex; idx < categories.length; idx++) {
		const cat = categories[idx];
		updateHeartbeat(idx);
		let catText = cat.text;
		let catHref = cat.href;
		if (catHref.startsWith('/')) {
			catHref = 'https://www.capterra.ch' + catHref;
		}
		console.log(`\nScraping category: ${catText}\nURL: ${catHref}`);
		let success = false;
		let retryCount = 0;
		const baseDelay = 10; // initial delay in seconds
		const maxDelay = 600; // maximum delay in seconds (10 minutes)
		while (!success) {
			updateHeartbeat(idx);
			try {
				const productLinks = await getCategoryProductLinks(catHref, page);
				results[catText] = productLinks;
				console.log(`Found ${productLinks.length} product links for '${catText}'`);
				// Append each found link to the CSV file
				for (const link of productLinks) {
					const row = `"${catText.replace(/"/g, '""')}","${link}"\n`;
					fs.appendFileSync(csvFilePath, row);
				}
				success = true;
			} catch (error) {
				console.error(`Error scraping category '${catText}': ${error}`);
				const retryDelay = Math.min(maxDelay, baseDelay * Math.pow(2, retryCount));
				console.log(`Retrying category '${catText}' after ${retryDelay} seconds...`);
				await sleep(retryDelay * 1000);
				retryCount++;
			}
		}
		updateProgress(idx);
	}
	await browser.close();
	return results;
}

// --- Main entry point ---
(async () => {
	const allProducts = await scrapeAllCategoriesProducts();
	console.log('\n--- All Categories and Their Product Links ---');
	for (const category in allProducts) {
		console.log(`\nCategory: ${category}`);
		console.log(`Total Products: ${allProducts[category].length}`);
		allProducts[category].forEach((link) => console.log(`  ${link}`));
	}
})();
