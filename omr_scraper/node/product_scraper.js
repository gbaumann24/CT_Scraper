#!/usr/bin/env node

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const AdblockerPlugin = require('puppeteer-extra-plugin-adblocker');
const fs = require('fs');
const cheerio = require('cheerio');

puppeteer.use(StealthPlugin());
puppeteer.use(AdblockerPlugin({ blockTrackers: true }));

// --- Helper: Sleep ---
function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

// --- Configuration: User Agents ---
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

// --- Proxy List (if needed) ---
const PROXIES = [];

// --- Files for Heartbeat and Progress ---
const HEARTBEAT_FILE = 'heartbeat_products.txt';
const PROGRESS_FILE = 'progress_products.txt';

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
			return isNaN(index) ? 0 : index + 1;
		} catch (err) {
			console.error('Error reading progress file:', err);
		}
	}
	return 0;
}

// --- Simulate human-like scrolling (with increased delays) ---
async function simulateHumanInteraction(page) {
	// Random scroll events: 3 to 6 times.
	const iterations = Math.floor(Math.random() * 4) + 3;
	for (let i = 0; i < iterations; i++) {
		// Wait between 1 and 3 seconds before each scroll.
		const waitTime = 1000 + Math.random() * 2000;
		// Scroll distance: between 100 and 300 pixels.
		const scrollDistance = 100 + Math.random() * 200;
		// 80% chance to scroll down, 20% chance up.
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
	// // Scroll to bottom.
	// await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
	// Wait between 5 and 10 seconds.
	await sleep(5000 + Math.random() * 5000);
}

// --- Set complex headers ---
async function setComplexHeaders(page) {
	const acceptLanguages = ['en-US,en;q=0.9', 'de-DE,de;q=0.9', 'fr-FR,fr;q=0.9'];
	const selectedAcceptLanguage = acceptLanguages[Math.floor(Math.random() * acceptLanguages.length)];
	await page.setExtraHTTPHeaders({
		'Accept-Language': selectedAcceptLanguage,
		Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
		'Cache-Control': 'max-age=0, private, must-revalidate',
		'Upgrade-Insecure-Requests': '1',
		Referer: 'https://www.google.com/',
		'Accept-Encoding': 'gzip, deflate, br',
	});
}

// --- Create and configure the Puppeteer browser/page ---
async function getDriver() {
	const launchOptions = {
		headless: true,
		args: ['--disable-gpu', '--lang=en-US', '--window-size=1280,720', '--incognito', '--disable-blink-features=AutomationControlled', '--no-sandbox', '--disable-setuid-sandbox'],
	};

	// Choose a random user agent.
	const userAgent = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];

	// Rotate proxy if available.
	if (PROXIES.length > 0) {
		const proxy = PROXIES[Math.floor(Math.random() * PROXIES.length)];
		console.log('Using proxy:', proxy);
		launchOptions.args.push(`--proxy-server=${proxy}`);
	}

	const browser = await puppeteer.launch(launchOptions);
	const page = await browser.newPage();
	await page.setUserAgent(userAgent);
	await setComplexHeaders(page);

	// Wait for browser initialization.
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
			const response = await page.goto(url, { waitUntil: 'networkidle2' });
			if (response) {
				const status = response.status();
				// Immediately back off if a rate-limit status is returned.
				if (status === 403 || status === 429) {
					const delaySec = Math.min(120 * Math.pow(2, attempt - 1), maxWait);
					totalWait += delaySec;
					if (totalWait >= maxWait) {
						console.log(`Max wait time of ${maxWait} seconds reached. Aborting ${url}.`);
						break;
					}
					console.log(`Rate limit detected on ${url} (status ${status}). Waiting for ${delaySec} seconds before retrying...`);
					await sleep(delaySec * 1000);
					attempt++;
					continue;
				}
			}

			// Dismiss cookie popover if it exists.
			try {
				const cookieButtonSelector = '#onetrust-accept-btn-handler';
				await page.waitForSelector(cookieButtonSelector, { timeout: 5000 });
				console.log('Cookie popover detected. Dismissing it.');
				await page.click(cookieButtonSelector);
				await sleep(2000);
			} catch (err) {
				// If not found, continue.
			}

			await simulateHumanInteraction(page);
			// Increase waiting time between requests: 25-35 seconds.
			const delay = 25000 + Math.random() * 10000;
			await sleep(delay);
			const pageContent = await page.content();

			// Check for indications of rate limiting or CAPTCHA in page content.
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
			const $ = cheerio.load(pageContent);
			// Check for reCAPTCHA elements (modify selectors as needed)
			const recaptchaElement = $('div.g-recaptcha, iframe[src*="recaptcha"]');
			if (recaptchaElement.length > 0) {
				console.log('CAPTCHA challenge detected on the page.');
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
			console.log(`Error loading ${url} on attempt ${attempt}: ${error.message}. Waiting for ${delaySec} seconds before next attempt...`);
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

	// Refined captcha detection using Cheerio:
	const $ = cheerio.load(pageContent);
	const recaptchaElement = $('div.g-recaptcha, iframe[src*="recaptcha"]');
	if (recaptchaElement.length > 0) {
		console.log(`CAPTCHA challenge detected on page ${url}. Skipping this page.`);
		return [];
	}

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

	// Wait between page requests: 25-35 seconds.
	const delay = 25000 + Math.random() * 10000;
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
	// Read categories from JSON file.
	let categories;
	try {
		const categoriesData = fs.readFileSync('capterra_categories.json', 'utf8');
		categories = JSON.parse(categoriesData);
	} catch (error) {
		console.error('No categories found in capterra_categories.json or error reading file');
		process.exit(1);
	}
	if (!categories || categories.length === 0) {
		console.log('No categories found in capterra_categories.json');
		process.exit(1);
	}

	// Parse command-line arguments for startFrom and direction.
	const args = process.argv.slice(2);
	let startFromIndex = 0;
	let direction = 'forward'; // default direction

	args.forEach((arg) => {
		if (arg.startsWith('--startFrom=')) {
			const value = arg.split('=')[1];
			if (value.toLowerCase() === 'half') {
				startFromIndex = Math.floor(categories.length / 2);
			} else {
				startFromIndex = parseInt(value, 10);
				if (isNaN(startFromIndex)) {
					console.error(`Invalid value for --startFrom: ${value}`);
					process.exit(1);
				}
			}
		} else if (arg.startsWith('--direction=')) {
			const value = arg.split('=')[1].toLowerCase();
			if (value === 'forward' || value === 'backward') {
				direction = value;
			} else {
				console.error(`Invalid value for --direction: ${value}`);
				process.exit(1);
			}
		}
	});

	// Adjust categories based on the startFrom index and direction.
	if (direction === 'forward') {
		// Process categories from startFromIndex to the end.
		categories = categories.slice(startFromIndex);
		console.log(`Processing categories from index ${startFromIndex} to end (forward).`);
	} else if (direction === 'backward') {
		// Process categories from startFromIndex down to 0.
		// Slice from 0 to startFromIndex+1 and then reverse the order.
		categories = categories.slice(0, startFromIndex + 1).reverse();
		console.log(`Processing categories from index ${startFromIndex} down to start (backward).`);
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
				// Append each found link to the CSV file.
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
